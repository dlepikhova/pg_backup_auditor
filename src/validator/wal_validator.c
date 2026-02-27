/*
 * wal_validator.c
 *
 * WAL archive validation logic
 *
 * Copyright (C) 2026 Daria Lepikhova
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#define _POSIX_C_SOURCE 200809L

#include "pg_backup_auditor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */

/*
 * Add error message to a ValidationResult
 */
static void
add_error(ValidationResult *result, const char *msg)
{
	if (result == NULL || msg == NULL)
		return;

	result->errors = realloc(result->errors,
							 sizeof(char *) * (result->error_count + 1));
	if (result->errors == NULL)
		return;

	result->errors[result->error_count] = strdup(msg);
	result->error_count++;
	result->status = BACKUP_STATUS_ERROR;
}

/*
 * Little-endian read helpers (used for WAL page header parsing)
 */
static uint16_t
read_u16le(const uint8_t *buf, int off)
{
	return (uint16_t)(buf[off] | ((uint16_t)buf[off + 1] << 8));
}

static uint32_t
read_u32le(const uint8_t *buf, int off)
{
	return (uint32_t)(buf[off]
					  | ((uint32_t)buf[off + 1] << 8)
					  | ((uint32_t)buf[off + 2] << 16)
					  | ((uint32_t)buf[off + 3] << 24));
}

static uint64_t
read_u64le(const uint8_t *buf, int off)
{
	return (uint64_t)buf[off]
		| ((uint64_t)buf[off + 1] << 8)
		| ((uint64_t)buf[off + 2] << 16)
		| ((uint64_t)buf[off + 3] << 24)
		| ((uint64_t)buf[off + 4] << 32)
		| ((uint64_t)buf[off + 5] << 40)
		| ((uint64_t)buf[off + 6] << 48)
		| ((uint64_t)buf[off + 7] << 56);
}

/*
 * XLogLongPageHeaderData field offsets (verified against real PG17 segment).
 *
 * XLogPageHeaderData (std, 20 bytes of fields, padded to 24 by the compiler
 * to satisfy the alignment of the embedded XLogRecPtr):
 *   offset  0: xlp_magic       (uint16)
 *   offset  2: xlp_info        (uint16)   XLP_LONG_HEADER = 0x0002
 *   offset  4: xlp_tli         (uint32)
 *   offset  8: xlp_pageaddr    (uint64)
 *   offset 16: xlp_rem_len     (uint32)
 *   offset 20: [4 bytes padding]
 * XLogLongPageHeaderData continuation:
 *   offset 24: xlp_sysid       (uint64)
 *   offset 32: xlp_seg_size    (uint32)
 *   offset 36: xlp_xlog_blcksz (uint32)
 * Total: 40 bytes
 */
#define WAL_LONG_HDR_SIZE   40
#define WAL_OFF_MAGIC        0
#define WAL_OFF_INFO         2
#define WAL_OFF_TLI          4
#define WAL_OFF_PAGEADDR     8
#define WAL_OFF_SEG_SIZE    32

/* First page of every WAL segment must have this flag in xlp_info */
#define XLP_LONG_HEADER  0x0002

/*
 * Read and validate the XLogLongPageHeaderData from one WAL segment.
 * Returns true if the header looks valid.
 */
static bool
validate_wal_segment_header(const char *seg_path,
							const char *seg_filename,
							uint32_t expected_tli,
							uint64_t expected_pageaddr,
							ValidationResult *result)
{
	FILE	   *f;
	uint8_t		buf[WAL_LONG_HDR_SIZE];
	size_t		n;
	uint16_t	xlp_magic;
	uint16_t	xlp_info;
	uint32_t	xlp_tli;
	uint64_t	xlp_pageaddr;
	uint32_t	xlp_seg_size;
	char		msg[512];
	bool		ok = true;

	f = fopen(seg_path, "rb");
	if (f == NULL)
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: cannot open for header check", seg_filename);
		add_error(result, msg);
		return false;
	}

	n = fread(buf, 1, sizeof(buf), f);
	fclose(f);

	if (n < sizeof(buf))
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: file too small to read header "
				 "(got %zu bytes, need %d)",
				 seg_filename, n, WAL_LONG_HDR_SIZE);
		add_error(result, msg);
		return false;
	}

	xlp_magic    = read_u16le(buf, WAL_OFF_MAGIC);
	xlp_info     = read_u16le(buf, WAL_OFF_INFO);
	xlp_tli      = read_u32le(buf, WAL_OFF_TLI);
	xlp_pageaddr = read_u64le(buf, WAL_OFF_PAGEADDR);
	xlp_seg_size = read_u32le(buf, WAL_OFF_SEG_SIZE);

	/* Magic must be non-zero */
	if (xlp_magic == 0)
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: zero magic value (not a WAL file?)",
				 seg_filename);
		add_error(result, msg);
		ok = false;
	}

	/* First page must have XLP_LONG_HEADER */
	if (!(xlp_info & XLP_LONG_HEADER))
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: XLP_LONG_HEADER flag missing "
				 "(xlp_info=0x%04X, magic=0x%04X)",
				 seg_filename, xlp_info, xlp_magic);
		add_error(result, msg);
		ok = false;
	}

	/* Timeline must match the backup */
	if (xlp_tli != expected_tli)
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: timeline mismatch "
				 "(header=%u, backup expects %u)",
				 seg_filename, xlp_tli, expected_tli);
		add_error(result, msg);
		ok = false;
	}

	/* Page address must match the start of this segment */
	if (xlp_pageaddr != expected_pageaddr)
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: page address mismatch "
				 "(header=0x%" PRIx64 ", expected=0x%" PRIx64 ")",
				 seg_filename, xlp_pageaddr, expected_pageaddr);
		add_error(result, msg);
		ok = false;
	}

	/* Segment size must be a power of two between 1 MB and 1 GB */
	if (xlp_seg_size == 0 ||
		(xlp_seg_size & (xlp_seg_size - 1)) != 0 ||
		xlp_seg_size < (1U << 20) ||
		xlp_seg_size > (1U << 30))
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: unexpected segment size in header "
				 "(xlp_seg_size=0x%X)",
				 seg_filename, xlp_seg_size);
		add_error(result, msg);
		ok = false;
	}

	if (ok)
		log_debug("WAL segment %s: header OK "
				  "(magic=0x%04X tli=%u seg_size=0x%X)",
				  seg_filename, xlp_magic, xlp_tli, xlp_seg_size);

	return ok;
}

/*
 * Check WAL continuity
 */
ValidationResult*
check_wal_continuity(WALArchiveInfo *wal_info)
{
	/* TODO: Implement WAL continuity check
	 *
	 * - Sort WAL segments by timeline, log_id, seg_id
	 * - Check for gaps in sequence
	 * - Detect timeline switches
	 * - Return validation result with any gaps found
	 */

	(void) wal_info;  /* unused for now */

	return NULL;
}

/*
 * Check if a WAL segment is present in the archive
 */
static bool
segment_exists_in_archive(WALSegmentName *seg, WALArchiveInfo *wal_info)
{
	int i;

	if (seg == NULL || wal_info == NULL || wal_info->segments == NULL)
		return false;

	/* Binary search would be more efficient, but linear search is simpler
	 * and segments are already sorted */
	for (i = 0; i < wal_info->segment_count; i++)
	{
		WALSegmentName *archived = &wal_info->segments[i];

		if (archived->timeline == seg->timeline &&
			archived->log_id == seg->log_id &&
			archived->seg_id == seg->seg_id)
		{
			return true;
		}
	}

	return false;
}

/*
 * Check if required WAL segments are available for backup
 */
ValidationResult*
check_wal_availability(BackupInfo *backup, WALArchiveInfo *wal_info)
{
	ValidationResult *result;
	WALSegmentName start_seg, stop_seg, current_seg;
	int missing_count = 0;
	char lsn_buf[64];
	char msg_buf[512];

	if (backup == NULL || wal_info == NULL)
		return NULL;

	/* Allocate result structure */
	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;

	result->status = BACKUP_STATUS_OK;
	result->error_count = 0;
	result->warning_count = 0;
	result->errors = NULL;
	result->warnings = NULL;

	/* Check if backup has LSN information */
	if (backup->start_lsn == 0 && backup->stop_lsn == 0)
	{
		result->status = BACKUP_STATUS_WARNING;
		result->warning_count = 1;
		result->warnings = malloc(sizeof(char *));
		result->warnings[0] = strdup("Backup has no LSN information");
		return result;
	}

	/* Convert LSNs to segment names
	 * Use default 16MB segment size (0x1000000) */
	lsn_to_seg(backup->start_lsn, backup->timeline, &start_seg, 0x1000000);
	lsn_to_seg(backup->stop_lsn, backup->timeline, &stop_seg, 0x1000000);

	log_debug("Checking WAL availability for backup %s", backup->backup_id);
	format_lsn(backup->start_lsn, lsn_buf, sizeof(lsn_buf));
	log_debug("  Start LSN: %s (timeline=%u, log=%08X, seg=%08X)",
			  lsn_buf, start_seg.timeline, start_seg.log_id, start_seg.seg_id);
	format_lsn(backup->stop_lsn, lsn_buf, sizeof(lsn_buf));
	log_debug("  Stop LSN:  %s (timeline=%u, log=%08X, seg=%08X)",
			  lsn_buf, stop_seg.timeline, stop_seg.log_id, stop_seg.seg_id);

	/* Check all segments from start to stop */
	current_seg = start_seg;

	while (current_seg.log_id < stop_seg.log_id ||
		   (current_seg.log_id == stop_seg.log_id && current_seg.seg_id <= stop_seg.seg_id))
	{
		if (!segment_exists_in_archive(&current_seg, wal_info))
		{
			missing_count++;

			/* Add error message */
			result->error_count++;
			result->errors = realloc(result->errors, result->error_count * sizeof(char *));

			snprintf(msg_buf, sizeof(msg_buf),
					 "Missing WAL segment: %08X%08X%08X",
					 current_seg.timeline, current_seg.log_id, current_seg.seg_id);
			result->errors[result->error_count - 1] = strdup(msg_buf);

			log_warning("%s", msg_buf);
		}

		/* Move to next segment */
		current_seg.seg_id++;

		/* Handle seg_id overflow */
		if (current_seg.seg_id == 0)
		{
			current_seg.log_id++;
		}

		/* Safety check: prevent infinite loop */
		if (current_seg.log_id > stop_seg.log_id + 1)
		{
			snprintf(msg_buf, sizeof(msg_buf),
					 "WAL range check aborted: too many segments");
			result->error_count++;
			result->errors = realloc(result->errors, result->error_count * sizeof(char *));
			result->errors[result->error_count - 1] = strdup(msg_buf);
			break;
		}
	}

	/* Set final status */
	if (missing_count > 0)
	{
		result->status = BACKUP_STATUS_ERROR;
		log_error("Backup %s is missing %d WAL segments", backup->backup_id, missing_count);
	}
	else
	{
		log_info("Backup %s has all required WAL segments", backup->backup_id);
	}

	return result;
}

/*
 * Find gaps in WAL archive
 */
typedef struct WALGap {
	WALSegmentName start;
	WALSegmentName end;
	struct WALGap *next;
} WALGap;

WALGap*
find_wal_gaps(WALArchiveInfo *wal_info)
{
	/* TODO: Implement gap detection
	 *
	 * - Iterate through sorted segments
	 * - When seg_id jumps by more than 1, record gap
	 * - Handle log_id overflow (seg_id wraps to 0)
	 * - Return linked list of gaps
	 */

	(void) wal_info;  /* unused for now */

	return NULL;
}

/*
 * Free WAL gap list
 */
void
free_wal_gaps(WALGap *gaps)
{
	WALGap *current = gaps;
	WALGap *next;

	while (current != NULL)
	{
		next = current->next;
		free(current);
		current = next;
	}
}

/*
 * Check WAL segment file headers for validity.
 *
 * For each required segment in the range [start_lsn, stop_lsn]:
 *   - Skip segments that are not present in the archive (check_wal_availability
 *     already handles reporting those as missing).
 *   - Open the segment and read the first 40 bytes (XLogLongPageHeaderData).
 *   - Validate: non-zero magic, XLP_LONG_HEADER flag, matching timeline,
 *     matching page address, and a plausible segment size value.
 *
 * NOTE: Uses hardcoded 16 MB (0x1000000) segment size — see BUG-002.
 */
ValidationResult*
check_wal_headers(BackupInfo *backup, WALArchiveInfo *wal_info)
{
	ValidationResult   *result;
	WALSegmentName		start_seg, stop_seg, cur;
	char				seg_filename[32];
	char				seg_path[PATH_MAX];
	int					checked = 0;

	if (backup == NULL || wal_info == NULL)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;

	result->status = BACKUP_STATUS_OK;

	if (backup->start_lsn == 0 && backup->stop_lsn == 0)
		return result;  /* No LSN info — nothing to check */

	lsn_to_seg(backup->start_lsn, backup->timeline, &start_seg, 0x1000000);
	lsn_to_seg(backup->stop_lsn,  backup->timeline, &stop_seg,  0x1000000);

	log_debug("Checking WAL headers for backup %s "
			  "(segments %08X%08X%08X .. %08X%08X%08X)",
			  backup->backup_id,
			  start_seg.timeline, start_seg.log_id, start_seg.seg_id,
			  stop_seg.timeline,  stop_seg.log_id,  stop_seg.seg_id);

	cur = start_seg;
	while (cur.log_id < stop_seg.log_id ||
		   (cur.log_id == stop_seg.log_id && cur.seg_id <= stop_seg.seg_id))
	{
		format_wal_filename(&cur, seg_filename, sizeof(seg_filename));
		path_join(seg_path, sizeof(seg_path),
				  wal_info->archive_path, seg_filename);

		if (file_exists(seg_path))
		{
			/*
			 * Expected page address = segment_number * segment_size
			 * where segment_number = log_id * 2^32 + seg_id
			 */
			uint64_t expected_pageaddr =
				((uint64_t)cur.log_id * 0x100000000ULL + (uint64_t)cur.seg_id)
				* (uint64_t)0x1000000;

			validate_wal_segment_header(seg_path, seg_filename,
										backup->timeline, expected_pageaddr,
										result);
			checked++;
		}
		/* Missing segments are skipped — check_wal_availability reports them */

		cur.seg_id++;
		if (cur.seg_id == 0)
			cur.log_id++;

		/* Safety: break if we somehow overshoot */
		if (cur.log_id > stop_seg.log_id + 1)
			break;
	}

	if (result->error_count == 0)
		log_info("Backup %s: WAL headers OK (%d segment%s checked)",
				 backup->backup_id, checked, checked == 1 ? "" : "s");
	else
		log_error("Backup %s: %d WAL header error%s (%d segment%s checked)",
				  backup->backup_id,
				  result->error_count, result->error_count == 1 ? "" : "s",
				  checked, checked == 1 ? "" : "s");

	return result;
}

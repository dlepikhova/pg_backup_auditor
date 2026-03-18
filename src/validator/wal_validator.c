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

/* -----------------------------------------------------------------------
 * CRC32C (Castagnoli) software implementation.
 * Polynomial: 0x82F63B78 (bit-reversed 0x1EDC6F41).
 * INIT = ~0U, FIN = ~crc — matches PostgreSQL's pg_crc32c.
 * ----------------------------------------------------------------------- */
static uint32_t crc32c_table[256];
static bool     crc32c_table_initialized = false;

static void
init_crc32c_table(void)
{
	const uint32_t poly = 0x82F63B78U;
	int i, j;

	for (i = 0; i < 256; i++)
	{
		uint32_t crc = (uint32_t) i;

		for (j = 0; j < 8; j++)
			crc = (crc & 1) ? ((crc >> 1) ^ poly) : (crc >> 1);
		crc32c_table[i] = crc;
	}
	crc32c_table_initialized = true;
}

/*
 * Update a running CRC32C accumulator over 'len' bytes.
 * Start with crc = ~0U; finalize with ~crc.
 */
static uint32_t
crc32c_update(uint32_t crc, const uint8_t *buf, size_t len)
{
	size_t i;

	if (!crc32c_table_initialized)
		init_crc32c_table();

	for (i = 0; i < len; i++)
		crc = (crc >> 8) ^ crc32c_table[(crc ^ buf[i]) & 0xFFU];

	return crc;
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
#define WAL_OFF_REM_LEN     16   /* xlp_rem_len */
#define WAL_OFF_SEG_SIZE    32
#define WAL_OFF_BLCKSZ      36   /* xlp_xlog_blcksz */

/* First page of every WAL segment must have this flag in xlp_info */
#define XLP_LONG_HEADER  0x0002

/* First XLogRecord starts right after the long page header */
#define WAL_RECORD_OFFSET   WAL_LONG_HDR_SIZE  /* 40 */

/* XLogRecord field offsets (within the record) */
#define WAL_XLOG_HDR_SIZE    24   /* sizeof(XLogRecord) */
#define WAL_XLOG_OFF_TOTLEN   0   /* xl_tot_len */
#define WAL_XLOG_OFF_CRC     20   /* xl_crc */

/* Buffer size for reading page header + first XLogRecord */
#define WAL_READ_BUF_SIZE  8192

/* Short page header on continuation pages (XLogPageHeaderData, MAXALIGN'd to 24) */
#define WAL_SHORT_HDR_SIZE  24

/* Align n up to the nearest 8-byte boundary (mirrors PostgreSQL MAXALIGN on 64-bit) */
#define WAL_MAXALIGN(n)  (((uint32_t)(n) + 7U) & ~7U)

/*
 * Read and validate the XLogLongPageHeaderData from one WAL segment,
 * plus CRC32C of the first XLogRecord if it fits in the read buffer.
 * Returns true if everything looks valid.
 */
static bool
validate_wal_segment_header(const char *seg_path,
							const char *seg_filename,
							uint32_t expected_tli,
							uint64_t expected_pageaddr,
							ValidationResult *result)
{
	FILE	   *f;
	uint8_t		buf[WAL_READ_BUF_SIZE];
	size_t		n;
	uint16_t	xlp_magic;
	uint16_t	xlp_info;
	uint32_t	xlp_tli;
	uint64_t	xlp_pageaddr;
	uint32_t	xlp_rem_len;
	uint32_t	xlp_seg_size;
	uint32_t	xlp_xlog_blcksz;
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

	if (n < (size_t) WAL_LONG_HDR_SIZE)
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: file too small to read header "
				 "(got %zu bytes, need %d)",
				 seg_filename, n, WAL_LONG_HDR_SIZE);
		add_error(result, msg);
		return false;
	}

	xlp_magic       = read_u16le(buf, WAL_OFF_MAGIC);
	xlp_info        = read_u16le(buf, WAL_OFF_INFO);
	xlp_tli         = read_u32le(buf, WAL_OFF_TLI);
	xlp_pageaddr    = read_u64le(buf, WAL_OFF_PAGEADDR);
	xlp_rem_len     = read_u32le(buf, WAL_OFF_REM_LEN);
	xlp_seg_size    = read_u32le(buf, WAL_OFF_SEG_SIZE);
	xlp_xlog_blcksz = read_u32le(buf, WAL_OFF_BLCKSZ);

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

	/*
	 * File size must equal the segment size declared in the header.
	 *
	 * We only do this check when xlp_seg_size itself is valid (power-of-two,
	 * 1 MB–1 GB), so we never chase a corrupt value.  Any segment in a
	 * proper archive must be complete; a truncated file cannot be replayed
	 * past the point of truncation.
	 *
	 * NOTE: The current segment in a live pg_wal directory is being written
	 * to and will be smaller than xlp_seg_size.  Archives should only contain
	 * completed segments, so truncation is an error in that context.
	 */
	if (xlp_seg_size != 0 &&
		(xlp_seg_size & (xlp_seg_size - 1)) == 0 &&
		xlp_seg_size >= (1U << 20) &&
		xlp_seg_size <= (1U << 30))
	{
		off_t actual_size = get_file_size(seg_path);

		if (actual_size >= 0 && (uint64_t)actual_size < (uint64_t)xlp_seg_size)
		{
			snprintf(msg, sizeof(msg),
					 "WAL segment %s: truncated "
					 "(%lld bytes, expected %u per header)",
					 seg_filename, (long long)actual_size, xlp_seg_size);
			add_error(result, msg);
			ok = false;
		}
	}

	/* Block size must be a power of two in [512, 65536] */
	if (xlp_xlog_blcksz == 0 ||
		(xlp_xlog_blcksz & (xlp_xlog_blcksz - 1)) != 0 ||
		xlp_xlog_blcksz < 512 ||
		xlp_xlog_blcksz > 65536)
	{
		snprintf(msg, sizeof(msg),
				 "WAL segment %s: invalid block size in header "
				 "(xlp_xlog_blcksz=%u)",
				 seg_filename, xlp_xlog_blcksz);
		add_error(result, msg);
		ok = false;
	}

	/*
	 * CRC32C check of the first XLogRecord.
	 *
	 * Only attempted when:
	 *   - The page header looks valid so far (ok == true).
	 *   - xlp_rem_len == 0, meaning the page starts with a complete record,
	 *     not a continuation fragment from the previous segment.
	 *   - We have read at least enough bytes to inspect the record header.
	 *
	 * PostgreSQL computes the CRC over:
	 *   - XLogRecord bytes [0..19]  (everything before xl_crc)
	 *   - XLogRecord bytes [24..xl_tot_len-1]  (record data payload)
	 */
	if (ok && xlp_rem_len == 0 &&
		n >= (size_t)(WAL_RECORD_OFFSET + WAL_XLOG_HDR_SIZE))
	{
		const uint8_t *rec      = buf + WAL_RECORD_OFFSET;
		uint32_t       xl_tot_len = read_u32le(rec, WAL_XLOG_OFF_TOTLEN);

		if (xl_tot_len == 0)
		{
			/*
			 * Zero xl_tot_len means the page tail is zero-filled: the segment
			 * is either pre-allocated but not yet written (valid for the
			 * current segment in pg_wal) or the archive contains only a page
			 * header with no records.  Not an error; validate_wal_segment_records
			 * will iterate through pages and find nothing to check.
			 */
			log_debug("WAL segment %s: first page zero-filled (no records)",
					  seg_filename);
		}
		else if (xl_tot_len < (uint32_t) WAL_XLOG_HDR_SIZE)
		{
			snprintf(msg, sizeof(msg),
					 "WAL segment %s: first XLogRecord has implausible "
					 "xl_tot_len=%u",
					 seg_filename, xl_tot_len);
			add_error(result, msg);
			ok = false;
		}
		else if ((size_t)(WAL_RECORD_OFFSET + xl_tot_len) <= n)
		{
			/* Record fully buffered — verify CRC32C */
			uint32_t stored_crc = read_u32le(rec, WAL_XLOG_OFF_CRC);

			/*
			 * xl_crc == 0 means the record was written without a CRC
			 * (e.g. pg_probackup synthetic WAL records).  Skip check.
			 */
			if (stored_crc == 0)
			{
				log_debug("WAL segment %s: first XLogRecord CRC is zero "
						  "— skipping CRC check", seg_filename);
			}
			else
			{
				uint32_t crc = ~0U;

				/* Bytes 0..19 of the record (header fields before xl_crc) */
				crc = crc32c_update(crc, rec, 20);

				/* Bytes 24..xl_tot_len-1 (data payload; skip xl_crc at 20..23) */
				if (xl_tot_len > (uint32_t) WAL_XLOG_HDR_SIZE)
					crc = crc32c_update(crc,
										rec + WAL_XLOG_HDR_SIZE,
										xl_tot_len - WAL_XLOG_HDR_SIZE);

				uint32_t computed_crc = ~crc;

				if (computed_crc != stored_crc)
				{
					snprintf(msg, sizeof(msg),
							 "WAL segment %s: first XLogRecord CRC mismatch "
							 "(stored=0x%08X, computed=0x%08X, xl_tot_len=%u)",
							 seg_filename, stored_crc, computed_crc, xl_tot_len);
					add_error(result, msg);
					ok = false;
				}
				else
					log_debug("WAL segment %s: first XLogRecord CRC OK "
							  "(xl_tot_len=%u)", seg_filename, xl_tot_len);
			}
		}
		else
		{
			/* Record doesn't fit in our read buffer — skip CRC check */
			log_debug("WAL segment %s: first XLogRecord too large for CRC check "
					  "(xl_tot_len=%u, buffered=%zu)",
					  seg_filename, xl_tot_len, n - WAL_RECORD_OFFSET);
		}
	}

	if (ok)
		log_debug("WAL segment %s: header OK "
				  "(magic=0x%04X tli=%u seg_size=0x%X blcksz=%u)",
				  seg_filename, xlp_magic, xlp_tli, xlp_seg_size,
				  xlp_xlog_blcksz);

	return ok;
}

/*
 * validate_wal_segment_records
 *
 * Read the WAL segment at seg_path page by page and validate the CRC32C of
 * every XLogRecord that fits entirely within one page.
 *
 * Records that span a page boundary (xl_tot_len > bytes remaining on the
 * current page) are intentionally skipped: their presence is already verified
 * by check_wal_availability(), and assembling cross-page fragments requires
 * significantly more complexity for rarely-encountered large records.
 *
 * Also skipped: records whose stored xl_crc == 0.  pg_probackup writes
 * synthetic WAL records without a checksum; treating them as errors would
 * produce false positives.
 *
 * Returns the number of records whose CRC was actually checked.
 * Errors are appended to *result.
 */
static int
validate_wal_segment_records(const char *seg_path,
							 const char *seg_filename,
							 ValidationResult *result)
{
	FILE	   *fp;
	uint8_t		hdr_peek[WAL_LONG_HDR_SIZE];
	uint8_t    *page_buf;
	size_t		n_read;
	uint32_t	blcksz;
	int			page_no = 0;
	int			records_checked = 0;
	char		msg[512];

	fp = fopen(seg_path, "rb");
	if (fp == NULL)
		return 0;

	/* Peek at the first page header to learn the block size. */
	n_read = fread(hdr_peek, 1, sizeof(hdr_peek), fp);
	if (n_read < (size_t) WAL_LONG_HDR_SIZE)
	{
		fclose(fp);
		return 0;	/* too small — already reported by validate_wal_segment_header */
	}

	blcksz = read_u32le(hdr_peek, WAL_OFF_BLCKSZ);
	if (blcksz < 512 || blcksz > 65536)
	{
		fclose(fp);
		return 0;	/* invalid — already reported */
	}

	page_buf = (uint8_t *) malloc(blcksz);
	if (page_buf == NULL)
	{
		fclose(fp);
		return 0;
	}

	rewind(fp);

	while ((n_read = fread(page_buf, 1, blcksz, fp)) >= (size_t) WAL_LONG_HDR_SIZE)
	{
		uint32_t	hdr_size   = (page_no == 0) ? WAL_LONG_HDR_SIZE : WAL_SHORT_HDR_SIZE;
		uint32_t	xlp_rem_len;
		uint32_t	rec_off;

		if (n_read < hdr_size)
			break;	/* truncated page header */

		xlp_rem_len = read_u32le(page_buf, WAL_OFF_REM_LEN);

		/*
		 * First complete record on this page starts after the page header
		 * plus any continuation bytes from a record that began on the previous
		 * page, aligned up to the next 8-byte boundary.
		 */
		rec_off = WAL_MAXALIGN(hdr_size + xlp_rem_len);

		while (rec_off + (uint32_t) WAL_XLOG_HDR_SIZE <= (uint32_t) n_read)
		{
			const uint8_t *rec        = page_buf + rec_off;
			uint32_t       xl_tot_len = read_u32le(rec, WAL_XLOG_OFF_TOTLEN);
			uint32_t       bytes_avail;
			uint32_t       xl_crc;

			if (xl_tot_len == 0)
				break;	/* zero-fill or end of valid records on this page */

			if (xl_tot_len < (uint32_t) WAL_XLOG_HDR_SIZE)
			{
				snprintf(msg, sizeof(msg),
						 "WAL segment %s page %d offset %u: "
						 "invalid xl_tot_len=%u (minimum %d)",
						 seg_filename, page_no, rec_off,
						 xl_tot_len, WAL_XLOG_HDR_SIZE);
				add_error(result, msg);
				break;
			}

			bytes_avail = (uint32_t) n_read - rec_off;
			if (xl_tot_len > bytes_avail)
				break;	/* record crosses page boundary — skip */

			xl_crc = read_u32le(rec, WAL_XLOG_OFF_CRC);

			if (xl_crc == 0)
			{
				/*
				 * Stored CRC is zero → synthetic record (e.g. written by
				 * pg_probackup).  Skip the checksum comparison.
				 */
				log_debug("WAL segment %s page %d offset %u: xl_crc=0, skipping",
						  seg_filename, page_no, rec_off);
			}
			else
			{
				uint32_t crc = ~0U;
				uint32_t computed_crc;

				if (!crc32c_table_initialized)
					init_crc32c_table();

				/* Bytes [0..19] — header fields before xl_crc */
				crc = crc32c_update(crc, rec, 20);

				/* Bytes [24..xl_tot_len-1] — data payload after xl_crc */
				if (xl_tot_len > (uint32_t) WAL_XLOG_HDR_SIZE)
					crc = crc32c_update(crc,
										rec + WAL_XLOG_HDR_SIZE,
										xl_tot_len - WAL_XLOG_HDR_SIZE);

				computed_crc = ~crc;

				if (computed_crc != xl_crc)
				{
					snprintf(msg, sizeof(msg),
							 "WAL segment %s page %d offset %u: "
							 "CRC mismatch (stored=0x%08X, computed=0x%08X, "
							 "xl_tot_len=%u)",
							 seg_filename, page_no, rec_off,
							 xl_crc, computed_crc, xl_tot_len);
					add_error(result, msg);
				}
			}

			records_checked++;
			rec_off += WAL_MAXALIGN(xl_tot_len);
		}

		page_no++;
	}

	free(page_buf);
	fclose(fp);

	log_debug("WAL segment %s: %d record%s checked across %d page%s",
			  seg_filename,
			  records_checked, records_checked == 1 ? "" : "s",
			  page_no, page_no == 1 ? "" : "s");

	return records_checked;
}

/* WAL gap (a contiguous range of missing segments within a timeline) */
typedef struct WALGap {
	WALSegmentName start;
	WALSegmentName end;
	struct WALGap *next;
} WALGap;

/* Forward declaration — defined after check_wal_continuity */
static void free_wal_gaps(WALGap *gaps);

/*
 * Find gaps in a sorted WAL archive.
 *
 * Iterates through the pre-sorted segment array and reports any range of
 * segment names that are absent between two consecutive present segments
 * (within the same timeline).  Timeline switches are not considered gaps.
 *
 * Returns a linked list of WALGap structs (or NULL if no gaps / nothing to
 * check).  Caller must free with free_wal_gaps().
 */
WALGap*
find_wal_gaps(WALArchiveInfo *wal_info)
{
	WALGap *gaps = NULL;
	WALGap *last_gap = NULL;
	int     i;

	if (wal_info == NULL || wal_info->segments == NULL ||
		wal_info->segment_count < 2)
		return NULL;

	for (i = 0; i < wal_info->segment_count - 1; i++)
	{
		WALSegmentName *cur = &wal_info->segments[i];
		WALSegmentName *nxt = &wal_info->segments[i + 1];

		/* Timeline switch — not a gap */
		if (cur->timeline != nxt->timeline)
			continue;

		/* Compute what the consecutive next segment should be */
		uint32_t exp_log = cur->log_id;
		uint32_t exp_seg = cur->seg_id + 1;

		if (exp_seg == 0)   /* seg_id wrapped */
			exp_log++;

		/* Consecutive — no gap */
		if (nxt->log_id == exp_log && nxt->seg_id == exp_seg)
			continue;

		/* Gap detected: missing range is [exp_log/exp_seg .. nxt-1] */
		WALGap *gap = calloc(1, sizeof(WALGap));
		if (gap == NULL)
			break;

		gap->start.timeline = cur->timeline;
		gap->start.log_id   = exp_log;
		gap->start.seg_id   = exp_seg;

		gap->end.timeline   = nxt->timeline;
		if (nxt->seg_id == 0)
		{
			gap->end.log_id = nxt->log_id - 1;
			gap->end.seg_id = UINT32_MAX;
		}
		else
		{
			gap->end.log_id = nxt->log_id;
			gap->end.seg_id = nxt->seg_id - 1;
		}

		gap->next = NULL;

		if (last_gap == NULL)
			gaps = gap;
		else
			last_gap->next = gap;
		last_gap = gap;
	}

	return gaps;
}

/*
 * Check WAL archive for gaps in segment continuity.
 *
 * Uses find_wal_gaps() on the pre-sorted segment list and reports every
 * missing range as an error in the returned ValidationResult.
 */
ValidationResult*
check_wal_continuity(WALArchiveInfo *wal_info)
{
	ValidationResult *result;
	WALGap           *gaps, *g;
	char              msg[256];

	if (wal_info == NULL)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;

	result->status = BACKUP_STATUS_OK;

	gaps = find_wal_gaps(wal_info);

	for (g = gaps; g != NULL; g = g->next)
	{
		if (g->start.log_id == g->end.log_id &&
			g->start.seg_id == g->end.seg_id)
			snprintf(msg, sizeof(msg),
					 "WAL gap: missing segment %08X%08X%08X",
					 g->start.timeline, g->start.log_id, g->start.seg_id);
		else
			snprintf(msg, sizeof(msg),
					 "WAL gap: missing segments "
					 "%08X%08X%08X .. %08X%08X%08X",
					 g->start.timeline, g->start.log_id, g->start.seg_id,
					 g->end.timeline,   g->end.log_id,   g->end.seg_id);

		add_error(result, msg);
		log_warning("%s", msg);
	}

	free_wal_gaps(gaps);

	if (result->error_count == 0)
		log_info("WAL archive continuity OK (%d segment%s)",
				 wal_info->segment_count,
				 wal_info->segment_count == 1 ? "" : "s");
	else
		log_error("WAL archive has %d gap%s",
				  result->error_count,
				  result->error_count == 1 ? "" : "s");

	return result;
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
	 * Use default 16MB segment size (0x1000000).
	 *
	 * Use redo_lsn as the effective start when it falls in an earlier segment
	 * than start_lsn.  redo_lsn is parsed from database/backup_label
	 * (START WAL LOCATION) and represents the oldest WAL location needed for
	 * recovery.  If it precedes start_lsn across a segment boundary the
	 * additional segment must also be present in the archive.
	 */
	{
		XLogRecPtr eff_start = backup->start_lsn;

		if (backup->redo_lsn != 0 && backup->redo_lsn < backup->start_lsn)
		{
			WALSegmentName redo_seg, chk_seg;

			lsn_to_seg(backup->redo_lsn,  backup->timeline, &redo_seg, 0x1000000);
			lsn_to_seg(backup->start_lsn, backup->timeline, &chk_seg,  0x1000000);

			if (redo_seg.log_id < chk_seg.log_id ||
				(redo_seg.log_id == chk_seg.log_id &&
				 redo_seg.seg_id  < chk_seg.seg_id))
			{
				eff_start = backup->redo_lsn;
				log_debug("Backup %s: redo_lsn precedes start_lsn by a segment "
						  "— extending WAL range to redo segment",
						  backup->backup_id);
			}
		}
		lsn_to_seg(eff_start, backup->timeline, &start_seg, 0x1000000);
	}
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
 * Free WAL gap list
 */
static void
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

	/* Use redo_lsn as effective start when it falls in an earlier segment */
	{
		XLogRecPtr eff_start = backup->start_lsn;

		if (backup->redo_lsn != 0 && backup->redo_lsn < backup->start_lsn)
		{
			WALSegmentName redo_seg, chk_seg;

			lsn_to_seg(backup->redo_lsn,  backup->timeline, &redo_seg, 0x1000000);
			lsn_to_seg(backup->start_lsn, backup->timeline, &chk_seg,  0x1000000);

			if (redo_seg.log_id < chk_seg.log_id ||
				(redo_seg.log_id == chk_seg.log_id &&
				 redo_seg.seg_id  < chk_seg.seg_id))
				eff_start = backup->redo_lsn;
		}
		lsn_to_seg(eff_start,         backup->timeline, &start_seg, 0x1000000);
	}
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

			int errors_before = result->error_count;

			validate_wal_segment_header(seg_path, seg_filename,
										backup->timeline, expected_pageaddr,
										result);

			/*
			 * Only validate individual record CRCs when the page header is
			 * clean — a corrupt header makes record offsets unreliable.
			 */
			if (result->error_count == errors_before)
				validate_wal_segment_records(seg_path, seg_filename, result);

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

/* -----------------------------------------------------------------------
 * WAL restore-chain continuity
 * ----------------------------------------------------------------------- */

/*
 * Parse a PostgreSQL timeline history file and return the switch LSN at which
 * the given parent_tli transitioned to new_tli.
 *
 * History file: NNNNNNNN.history in the WAL archive.
 * Non-comment lines: <parent_tli><whitespace><switch_lsn><whitespace><reason>
 * Example: "1\t0/5000000\tno recovery target specified\n"
 *
 * Returns true and sets *switch_lsn_out if the entry is found, false if the
 * file is absent or the parent timeline entry is missing / unparseable.
 */
static bool
parse_history_switchpoint(const char *archive_path, uint32_t new_tli,
						  uint32_t parent_tli, XLogRecPtr *switch_lsn_out)
{
	char   history_filename[32];
	char   history_path[PATH_MAX];
	FILE  *fp;
	char   line[256];

	snprintf(history_filename, sizeof(history_filename),
			 "%08X.history", new_tli);
	path_join(history_path, sizeof(history_path),
			  archive_path, history_filename);

	fp = fopen(history_path, "r");
	if (fp == NULL)
		return false;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		char         *p = line;
		char         *end;
		unsigned long tli_val;
		char          lsn_buf[32];
		size_t        lsn_len;

		/* Skip leading whitespace */
		while (*p == ' ' || *p == '\t') p++;

		/* Skip comment / blank lines */
		if (*p == '#' || *p == '\n' || *p == '\r' || *p == '\0')
			continue;

		/* Parse parent timeline number */
		tli_val = strtoul(p, &end, 10);
		if (end == p)
			continue;	/* no digits — malformed line */

		if ((uint32_t)tli_val != parent_tli)
			continue;

		/* Skip whitespace between tli and LSN */
		p = end;
		while (*p == ' ' || *p == '\t') p++;

		/* Extract the LSN token (non-whitespace, non-newline chars) */
		end = p;
		while (*end && *end != ' ' && *end != '\t' &&
			   *end != '\n' && *end != '\r')
			end++;

		lsn_len = (size_t)(end - p);
		if (lsn_len == 0 || lsn_len >= sizeof(lsn_buf))
			continue;

		memcpy(lsn_buf, p, lsn_len);
		lsn_buf[lsn_len] = '\0';

		if (parse_lsn(lsn_buf, switch_lsn_out))
		{
			fclose(fp);
			return true;
		}
	}

	fclose(fp);
	return false;
}

/*
 * qsort comparator: sort BackupInfo pointers by (timeline, stop_lsn).
 * Determines the order in which consecutive backups should be bridged.
 */
static int
compare_backup_by_lsn(const void *a, const void *b)
{
	const BackupInfo *ba = *(const BackupInfo * const *)a;
	const BackupInfo *bb = *(const BackupInfo * const *)b;

	if (ba->timeline < bb->timeline) return -1;
	if (ba->timeline > bb->timeline) return  1;
	if (ba->stop_lsn < bb->stop_lsn) return -1;
	if (ba->stop_lsn > bb->stop_lsn) return  1;
	return 0;
}

/*
 * Verify that WAL is continuously available between every pair of consecutive
 * backups on the same timeline.
 *
 * For each consecutive pair (prev, next) sorted by (timeline, stop_lsn):
 *   - Bridge range: segments [lsn_to_seg(prev.stop_lsn)+1,
 *                              lsn_to_seg(next.start_lsn)-1]
 *   - Every segment in this range must exist in the archive.
 *
 * The range is intentionally exclusive of both endpoints:
 *   - prev's stop segment is already covered by check_wal_availability for prev.
 *   - next's start segment is already covered by check_wal_availability for next.
 *
 * If bridge_start >= bridge_end the backups are adjacent or overlapping and no
 * bridge check is needed.
 *
 * NOTE: Uses hardcoded 16 MB (0x1000000) segment size — see BUG-002.
 */
ValidationResult*
check_wal_restore_chain(BackupInfo *backups, WALArchiveInfo *wal_info)
{
	ValidationResult   *result;
	BackupInfo        **arr;
	int					n, i;
	int					chain_errors = 0;
	char				msg[512];
	char				lsn_a[32], lsn_b[32];

	if (backups == NULL || wal_info == NULL)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;

	result->status = BACKUP_STATUS_OK;

	/*
	 * Count backups eligible for chain checking: must have valid LSN info
	 * and must not be ORPHAN.  Orphan backups have a broken chain by
	 * definition (their parent is gone), so including them would generate
	 * spurious bridge errors for every gap left by the missing parent.
	 */
	n = 0;
	for (BackupInfo *b = backups; b != NULL; b = b->next)
		if (b->start_lsn > 0 && b->stop_lsn > 0 &&
			b->status != BACKUP_STATUS_ORPHAN)
			n++;

	if (n < 2)
	{
		log_debug("WAL restore chain: fewer than 2 eligible backups — skipping");
		return result;
	}

	arr = malloc((size_t)n * sizeof(*arr));
	if (arr == NULL)
		return result;

	i = 0;
	for (BackupInfo *b = backups; b != NULL; b = b->next)
		if (b->start_lsn > 0 && b->stop_lsn > 0 &&
			b->status != BACKUP_STATUS_ORPHAN)
			arr[i++] = b;

	qsort(arr, (size_t)n, sizeof(*arr), compare_backup_by_lsn);

	for (i = 0; i < n - 1; i++)
	{
		BackupInfo     *prev = arr[i];
		BackupInfo     *next = arr[i + 1];
		WALSegmentName	stop_seg, start_seg, cur;

		if (prev->timeline != next->timeline)
		{
			/*
			 * Cross-timeline bridge: to recover from prev (on tli A) to
			 * next (on tli B) we need:
			 *  1. WAL on tli A from prev.stop_lsn to the switch point
			 *     (pre-switch bridge).
			 *  2. WAL on tli B from the switch point to next.start_lsn
			 *     (post-switch bridge; may be empty).
			 *
			 * The switch LSN is read from next->timeline's history file.
			 */
			XLogRecPtr		switch_lsn;
			WALSegmentName	prev_stop_seg, switch_seg_old, switch_seg_new,
							next_start_seg, bridge_cur;
			char			lsn_sw[32];

			if (!parse_history_switchpoint(wal_info->archive_path,
										  next->timeline, prev->timeline,
										  &switch_lsn))
			{
				snprintf(msg, sizeof(msg),
						 "WAL cross-timeline chain: cannot determine switch "
						 "point tli=%u → tli=%u: %08X.history missing or "
						 "entry for tli=%u not found",
						 prev->timeline, next->timeline,
						 next->timeline, prev->timeline);
				add_error(result, msg);
				chain_errors++;
				continue;
			}

			format_lsn(switch_lsn, lsn_sw, sizeof(lsn_sw));
			lsn_to_seg(prev->stop_lsn,  prev->timeline, &prev_stop_seg,  0x1000000);
			lsn_to_seg(switch_lsn,      prev->timeline, &switch_seg_old, 0x1000000);
			lsn_to_seg(switch_lsn,      next->timeline, &switch_seg_new, 0x1000000);
			lsn_to_seg(next->start_lsn, next->timeline, &next_start_seg, 0x1000000);

			/* --- 1. Pre-switch bridge on prev->timeline ---
			 * Range: [prev_stop_seg+1, switch_seg_old]  (inclusive) */
			bridge_cur = prev_stop_seg;
			bridge_cur.seg_id++;
			if (bridge_cur.seg_id == 0) bridge_cur.log_id++;

			while (bridge_cur.log_id < switch_seg_old.log_id ||
				   (bridge_cur.log_id == switch_seg_old.log_id &&
					bridge_cur.seg_id <= switch_seg_old.seg_id))
			{
				if (!segment_exists_in_archive(&bridge_cur, wal_info))
				{
					char seg_fname[32];
					format_wal_filename(&bridge_cur, seg_fname, sizeof(seg_fname));
					format_lsn(prev->stop_lsn, lsn_a, sizeof(lsn_a));
					snprintf(msg, sizeof(msg),
							 "WAL cross-timeline chain broken: backup %s "
							 "(tli=%u, stop=%s) → %s (tli=%u): "
							 "missing pre-switch segment %s (switch at %s)",
							 prev->backup_id, prev->timeline, lsn_a,
							 next->backup_id, next->timeline,
							 seg_fname, lsn_sw);
					add_error(result, msg);
					chain_errors++;
				}
				bridge_cur.seg_id++;
				if (bridge_cur.seg_id == 0) bridge_cur.log_id++;
				if (bridge_cur.log_id > switch_seg_old.log_id + 1) break;
			}

			/* --- 2. Post-switch bridge on next->timeline ---
			 * Range: [switch_seg_new+1, next_start_seg-1]  (exclusive next.start)
			 * Empty when switch and next.start fall in the same or adjacent
			 * segment. */
			bridge_cur = switch_seg_new;
			bridge_cur.seg_id++;
			if (bridge_cur.seg_id == 0) bridge_cur.log_id++;

			while (bridge_cur.log_id < next_start_seg.log_id ||
				   (bridge_cur.log_id == next_start_seg.log_id &&
					bridge_cur.seg_id < next_start_seg.seg_id))
			{
				if (!segment_exists_in_archive(&bridge_cur, wal_info))
				{
					char seg_fname[32];
					format_wal_filename(&bridge_cur, seg_fname, sizeof(seg_fname));
					format_lsn(next->start_lsn, lsn_b, sizeof(lsn_b));
					snprintf(msg, sizeof(msg),
							 "WAL cross-timeline chain broken: backup %s "
							 "(tli=%u, start=%s): missing post-switch segment "
							 "%s on tli=%u (switch at %s)",
							 next->backup_id, next->timeline, lsn_b,
							 seg_fname, next->timeline, lsn_sw);
					add_error(result, msg);
					chain_errors++;
				}
				bridge_cur.seg_id++;
				if (bridge_cur.seg_id == 0) bridge_cur.log_id++;
				if (bridge_cur.log_id > next_start_seg.log_id + 1) break;
			}

			continue;
		}

		lsn_to_seg(prev->stop_lsn,  prev->timeline, &stop_seg,  0x1000000);
		lsn_to_seg(next->start_lsn, next->timeline, &start_seg, 0x1000000);

		/*
		 * Bridge starts one segment after prev's stop segment.
		 * Bridge ends one segment before next's start segment.
		 * If bridge_start >= start_seg the backups are adjacent — nothing
		 * to bridge.
		 */
		cur = stop_seg;
		cur.seg_id++;
		if (cur.seg_id == 0)
			cur.log_id++;

		if (cur.log_id > start_seg.log_id ||
			(cur.log_id == start_seg.log_id && cur.seg_id >= start_seg.seg_id))
			continue;	/* adjacent or overlapping — no bridge gap possible */

		/* Iterate over bridge range and report any missing segment */
		while (cur.log_id < start_seg.log_id ||
			   (cur.log_id == start_seg.log_id && cur.seg_id < start_seg.seg_id))
		{
			if (!segment_exists_in_archive(&cur, wal_info))
			{
				char seg_filename[32];

				format_wal_filename(&cur, seg_filename, sizeof(seg_filename));
				format_lsn(prev->stop_lsn,  lsn_a, sizeof(lsn_a));
				format_lsn(next->start_lsn, lsn_b, sizeof(lsn_b));

				snprintf(msg, sizeof(msg),
						 "WAL chain broken: backup %s (stop=%s) → %s (start=%s): "
						 "missing bridge segment %s",
						 prev->backup_id, lsn_a,
						 next->backup_id, lsn_b,
						 seg_filename);
				add_error(result, msg);
				chain_errors++;
			}

			cur.seg_id++;
			if (cur.seg_id == 0)
				cur.log_id++;

			/* Safety: prevent runaway loop on malformed input */
			if (cur.log_id > start_seg.log_id + 1)
				break;
		}
	}

	free(arr);

	if (chain_errors == 0)
		log_info("WAL restore chain: all inter-backup bridges complete "
				 "(%d backup%s checked)", n, n == 1 ? "" : "s");
	else
		log_error("WAL restore chain: %d missing segment%s break the recovery chain",
				  chain_errors, chain_errors == 1 ? "" : "s");

	return result;
}

/*
 * Check WAL segment file headers for ALL segments in the archive.
 *
 * Unlike check_wal_headers() (which only validates segments within a specific
 * backup's LSN window), this function iterates over every segment present in
 * the archive and validates its page header.  It detects corruptions such as
 * segment-file swaps, where the content of one segment ends up in another
 * file — something that check_wal_headers() misses for segments that fall
 * between backups.
 *
 * For each segment the expected page address is:
 *   (log_id * 2^32 + seg_id) * 0x1000000
 * using the hardcoded 16 MB segment size (see BUG-002).
 *
 * Per-record CRC validation is also run on each segment whose page header
 * passes cleanly.
 *
 * NOTE: Uses hardcoded 16 MB (0x1000000) segment size — see BUG-002.
 */
ValidationResult*
check_wal_archive_headers(WALArchiveInfo *wal_info)
{
	ValidationResult   *result;
	int					i, checked = 0;
	char				seg_filename[32];
	char				seg_path[PATH_MAX];

	if (wal_info == NULL)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;

	result->status = BACKUP_STATUS_OK;

	for (i = 0; i < wal_info->segment_count; i++)
	{
		WALSegmentName *seg = &wal_info->segments[i];

		format_wal_filename(seg, seg_filename, sizeof(seg_filename));
		path_join(seg_path, sizeof(seg_path),
				  wal_info->archive_path, seg_filename);

		if (!file_exists(seg_path))
			continue;

		uint64_t expected_pageaddr =
			((uint64_t)seg->log_id * 0x100000000ULL + (uint64_t)seg->seg_id)
			* (uint64_t)0x1000000;

		int errors_before = result->error_count;

		validate_wal_segment_header(seg_path, seg_filename,
									seg->timeline, expected_pageaddr,
									result);

		/*
		 * Only run per-record CRC validation when the page header is clean —
		 * a corrupt header makes record offsets unreliable.
		 */
		if (result->error_count == errors_before)
			validate_wal_segment_records(seg_path, seg_filename, result);

		checked++;
	}

	if (result->error_count == 0)
		log_info("WAL archive headers OK (%d segment%s checked)",
				 checked, checked == 1 ? "" : "s");
	else
		log_error("WAL archive has %d header error%s (%d segment%s checked)",
				  result->error_count, result->error_count == 1 ? "" : "s",
				  checked, checked == 1 ? "" : "s");

	return result;
}

/*
 * Check that a timeline history file exists in the WAL archive.
 *
 * For timeline 1 there is no history file, so the check is skipped.
 * For timeline N > 1, PostgreSQL writes an N-digit hex file named
 * NNNNNNNN.history (e.g. "00000002.history") to the WAL archive
 * when a standby is promoted.  Its absence means the archive is
 * incomplete or the backup was taken on an untracked timeline branch.
 */
ValidationResult*
check_wal_timeline(BackupInfo *backup, WALArchiveInfo *wal_info)
{
	ValidationResult *result;
	char              history_filename[32];
	char              history_path[PATH_MAX];
	char              msg[512];

	if (backup == NULL || wal_info == NULL)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;

	result->status = BACKUP_STATUS_OK;

	/* Timeline 1 never has a history file */
	if (backup->timeline <= 1)
	{
		log_debug("Backup %s: timeline 1 — no history file expected",
				  backup->backup_id);
		return result;
	}

	/* History file name: 8 hex digits + ".history" */
	snprintf(history_filename, sizeof(history_filename),
			 "%08X.history", backup->timeline);

	path_join(history_path, sizeof(history_path),
			  wal_info->archive_path, history_filename);

	if (!file_exists(history_path))
	{
		snprintf(msg, sizeof(msg),
				 "Timeline %u history file missing in WAL archive: %s",
				 backup->timeline, history_filename);
		add_error(result, msg);
		log_error("Backup %s: %s", backup->backup_id, msg);
	}
	else
		log_info("Backup %s: timeline history file %s present",
				 backup->backup_id, history_filename);

	return result;
}

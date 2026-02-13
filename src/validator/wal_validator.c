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


#include "pg_backup_auditor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

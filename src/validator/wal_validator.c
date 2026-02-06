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
 * Check if required WAL segments are available for backup
 */
ValidationResult*
check_wal_availability(BackupInfo *backup, WALArchiveInfo *wal_info)
{
	/* TODO: Implement WAL availability check
	 *
	 * - Determine required WAL range (backup->start_lsn to backup->stop_lsn)
	 * - Check if all required segments are in archive
	 * - Return validation result
	 */

	(void) backup;  /* unused for now */
	(void) wal_info;  /* unused for now */

	return NULL;
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

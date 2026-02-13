/*
 * xlog.c
 *
 * LSN and WAL utilities
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
#include <ctype.h>
#include <stdbool.h>

/*
 * Parse LSN from string format "X/X"
 * Returns true on success, false on error
 */
bool
parse_lsn(const char *str, XLogRecPtr *lsn)
{
	char *endptr;
	unsigned long upper, lower;
	char *slash_pos;

	if (str == NULL || lsn == NULL)
		return false;

	/* Find the slash separator */
	slash_pos = strchr(str, '/');
	if (slash_pos == NULL)
		return false;

	/* Parse upper part (before slash) as hex */
	upper = strtoul(str, &endptr, 16);
	if (endptr != slash_pos)
		return false;

	/* Parse lower part (after slash) as hex */
	lower = strtoul(slash_pos + 1, &endptr, 16);
	if (*endptr != '\0')
		return false;

	/* Combine into 64-bit LSN */
	*lsn = ((uint64_t)upper << 32) | lower;

	return true;
}

/*
 * Compare two LSN values
 * Returns: -1 if lsn1 < lsn2, 0 if equal, 1 if lsn1 > lsn2
 */
int
lsn_compare(XLogRecPtr lsn1, XLogRecPtr lsn2)
{
	if (lsn1 < lsn2)
		return -1;
	if (lsn1 > lsn2)
		return 1;
	return 0;
}

/*
 * Format LSN as string
 */
void
format_lsn(XLogRecPtr lsn, char *buf, size_t bufsize)
{
	snprintf(buf, bufsize, "%X/%X",
			 (uint32_t) (lsn >> 32),
			 (uint32_t) lsn);
}

/*
 * Convert LSN to WAL segment name
 *
 * WAL segment size is 16MB (0x1000000) by default.
 * LSN is divided by segment size to get segment number.
 * Segment number is split into log_id (upper 32 bits) and seg_id (lower 32 bits).
 */
void
lsn_to_seg(XLogRecPtr lsn, uint32_t timeline, WALSegmentName *seg, uint32_t wal_segment_size)
{
	uint64_t segment_number;

	if (seg == NULL)
		return;

	/* Default WAL segment size is 16MB if not specified */
	if (wal_segment_size == 0)
		wal_segment_size = 0x1000000;  /* 16MB */

	/* Calculate segment number: LSN / WAL_SEGMENT_SIZE */
	segment_number = lsn / wal_segment_size;

	/* Split into log_id and seg_id
	 * In PostgreSQL 11+: segment number is 64-bit
	 *   Upper 32 bits = log_id (0xFFFFFFFF00000000)
	 *   Lower 8 bits of upper half + upper 24 bits of lower half = seg_id
	 *
	 * For simplicity, we use the traditional approach:
	 *   log_id = segment_number / 0x100000000
	 *   seg_id = segment_number % 0x100000000
	 */
	seg->timeline = timeline;
	seg->log_id = (uint32_t)(segment_number / 0x100000000ULL);
	seg->seg_id = (uint32_t)(segment_number % 0x100000000ULL);
}

/*
 * Check if string is valid hex
 */
static bool
is_hex_string(const char *str, int len)
{
	int i;

	for (i = 0; i < len; i++)
	{
		if (!isxdigit((unsigned char) str[i]))
			return false;
	}

	return true;
}

/*
 * Parse WAL filename
 * Returns true on success, false on error
 */
bool
parse_wal_filename(const char *filename, WALSegmentName *result)
{
	size_t len;
	char timeline_str[9];
	char log_id_str[9];
	char seg_id_str[9];
	unsigned long timeline, log_id, seg_id;
	char *endptr;

	if (filename == NULL || result == NULL)
		return false;

	/* WAL filename format: TTTTTTTTLLLLLLLLSSSSSSSS (24 hex chars)
	 * where:
	 *   TTTTTTTT = timeline ID (8 hex digits)
	 *   LLLLLLLL = log file ID (8 hex digits)
	 *   SSSSSSSS = segment ID (8 hex digits)
	 */
	len = strlen(filename);

	/* Check if filename is exactly 24 characters */
	if (len != 24)
		return false;

	/* Check if all characters are hex digits */
	if (!is_hex_string(filename, 24))
		return false;

	/* Extract timeline (first 8 chars) */
	memcpy(timeline_str, filename, 8);
	timeline_str[8] = '\0';
	timeline = strtoul(timeline_str, &endptr, 16);
	if (*endptr != '\0')
		return false;

	/* Extract log ID (next 8 chars) */
	memcpy(log_id_str, filename + 8, 8);
	log_id_str[8] = '\0';
	log_id = strtoul(log_id_str, &endptr, 16);
	if (*endptr != '\0')
		return false;

	/* Extract segment ID (last 8 chars) */
	memcpy(seg_id_str, filename + 16, 8);
	seg_id_str[8] = '\0';
	seg_id = strtoul(seg_id_str, &endptr, 16);
	if (*endptr != '\0')
		return false;

	/* Store results */
	result->timeline = (uint32_t)timeline;
	result->log_id = (uint32_t)log_id;
	result->seg_id = (uint32_t)seg_id;

	return true;
}

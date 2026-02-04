/*
 * xlog.c
 *
 * LSN and WAL utilities
 *
 * Copyright (C) 2026  Daria
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
 * Parse WAL filename
 * Returns true on success, false on error
 */
bool
parse_wal_filename(const char *filename, WALSegmentName *result)
{
	/* TODO: Implement WAL filename parsing
	 *
	 * Expected format: "000000010000000000000012" (24 hex chars)
	 * - Timeline: first 8 chars
	 * - Log ID: next 8 chars
	 * - Seg ID: last 8 chars
	 */

	(void) filename;  /* unused for now */
	(void) result;  /* unused for now */

	return false;
}

/*
 * Check if string is valid hex
 */
static bool __attribute__((unused))
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

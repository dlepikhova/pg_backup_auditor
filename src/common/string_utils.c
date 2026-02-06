/*
 * string_utils.c
 *
 * String manipulation utilities
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

/*
 * Trim whitespace from both ends of string
 */
char *
str_trim(char *str)
{
	char *end;

	/* Trim leading space */
	while (isspace((unsigned char)*str))
		str++;

	if (*str == 0)
		return str;

	/* Trim trailing space */
	end = str + strlen(str) - 1;
	while (end > str && isspace((unsigned char)*end))
		end--;

	/* Write new null terminator */
	end[1] = '\0';

	return str;
}

/*
 * Split string by delimiter
 * Note: modifies the input string
 */
char **
str_split(char *str, char delimiter, int *count)
{
	/* TODO: Implement string splitting
	 *
	 * - Count occurrences of delimiter
	 * - Allocate array
	 * - Replace delimiters with \0
	 * - Store pointers
	 */

	(void) str;  /* unused for now */
	(void) delimiter;  /* unused for now */

	*count = 0;
	return NULL;
}

/*
 * Safe string copy
 */
void
str_copy(char *dest, const char *src, size_t destsize)
{
	size_t len = strlen(src);

	if (len >= destsize)
		len = destsize - 1;

	memcpy(dest, src, len);
	dest[len] = '\0';
}

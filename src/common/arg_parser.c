/*
 * arg_parser.c
 *
 * Common argument parsing utilities
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


#include "arg_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <time.h>

/*
 * Check if an option has been seen multiple times and report error
 * Returns true if this is a duplicate (error condition)
 */
bool
check_duplicate_option(bool seen, const char *option_name)
{
	if (seen)
	{
		fprintf(stderr, "Error: %s specified multiple times\n", option_name);
		return true;
	}
	return false;
}

/*
 * Parse a string-valued CLI option: enforce no duplicates, then assign.
 * Returns false (and reports an error) if the option was already seen.
 */
bool
parse_string_option(const char *option_name, const char *value,
					char **target, bool *seen)
{
	if (check_duplicate_option(*seen, option_name))
		return false;
	*target = (char *)value;
	*seen = true;
	return true;
}

/*
 * Parse a DATE-valued CLI option in strict DD-MM-YYYY format.
 * See arg_parser.h for the full contract.
 */
bool
parse_date_option(const char *option_name, const char *value,
				  time_t *target, bool *seen)
{
	if (check_duplicate_option(*seen, option_name))
		return false;

	/* Strict format: exactly "DD-MM-YYYY" — 10 chars, dashes at
	 * positions 2 and 5, digits everywhere else. Zero-padding is
	 * required (e.g. "08-01-2026", not "8-1-2026"). */
	if (value == NULL || strlen(value) != 10 ||
		value[2] != '-' || value[5] != '-' ||
		!isdigit((unsigned char)value[0]) || !isdigit((unsigned char)value[1]) ||
		!isdigit((unsigned char)value[3]) || !isdigit((unsigned char)value[4]) ||
		!isdigit((unsigned char)value[6]) || !isdigit((unsigned char)value[7]) ||
		!isdigit((unsigned char)value[8]) || !isdigit((unsigned char)value[9]))
	{
		fprintf(stderr,
				"Error: Invalid date format for %s: '%s'. "
				"Expected DD-MM-YYYY (e.g. 15-08-2026)\n",
				option_name, value ? value : "(null)");
		return false;
	}

	int day   = (value[0] - '0') * 10 + (value[1] - '0');
	int month = (value[3] - '0') * 10 + (value[4] - '0');
	int year  = (value[6] - '0') * 1000 + (value[7] - '0') * 100 +
				(value[8] - '0') * 10   + (value[9] - '0');

	if (year < 1970 || year > 2100)
	{
		fprintf(stderr,
				"Error: Year out of range for %s: %d. Expected 1970..2100\n",
				option_name, year);
		return false;
	}

	struct tm tm = {
		.tm_mday  = day,
		.tm_mon   = month - 1,     /* tm_mon is 0..11 */
		.tm_year  = year - 1900,   /* tm_year is years since 1900 */
		.tm_isdst = -1,            /* let mktime resolve DST */
	};

	time_t t = mktime(&tm);
	/* mktime silently normalizes overflow (e.g. day 31 in a 30-day
	 * month, or Feb 30). Check that the parsed fields survived
	 * normalization to catch invalid calendar dates. */
	if (t == (time_t)-1 ||
		tm.tm_mday != day ||
		tm.tm_mon  != month - 1 ||
		tm.tm_year != year - 1900)
	{
		fprintf(stderr,
				"Error: Invalid calendar date for %s: '%s'\n",
				option_name, value);
		return false;
	}

	*target = t;
	*seen = true;
	return true;
}

/*
 * Parse an integer argument and validate it
 * Returns true on success, false on error
 */
bool
parse_int_argument(const char *str, int *result, const char *option_name)
{
	char *endptr;
	long val;

	errno = 0;
	val = strtol(str, &endptr, 10);

	/* Check for various possible errors */
	if (errno != 0 || *endptr != '\0')
	{
		fprintf(stderr, "Error: Invalid integer value for %s: %s\n", option_name, str);
		return false;
	}

	if (val < INT_MIN || val > INT_MAX)
	{
		fprintf(stderr, "Error: Value out of range for %s: %ld\n", option_name, val);
		return false;
	}

	*result = (int)val;
	return true;
}

/*
 * Validate that a required argument is provided
 * Returns true if valid, false if missing
 */
bool
validate_required_option(const char *value, const char *option_name)
{
	if (value == NULL)
	{
		fprintf(stderr, "Error: %s is required\n", option_name);
		return false;
	}
	return true;
}

/*
 * Check if exactly one of two options is provided
 * Returns true if valid, false if both or neither are provided
 */
bool
validate_exclusive_options(bool opt1_set, bool opt2_set,
                           const char *opt1_name, const char *opt2_name)
{
	if (opt1_set && opt2_set)
	{
		fprintf(stderr, "Error: %s and %s are mutually exclusive\n",
				opt1_name, opt2_name);
		return false;
	}

	if (!opt1_set && !opt2_set)
	{
		fprintf(stderr, "Error: Either %s or %s must be specified\n",
				opt1_name, opt2_name);
		return false;
	}

	return true;
}

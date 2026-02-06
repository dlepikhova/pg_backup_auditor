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
#include <errno.h>
#include <limits.h>

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

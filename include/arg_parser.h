/*
 * arg_parser.h
 *
 * Common argument parsing utilities
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


#ifndef ARG_PARSER_H
#define ARG_PARSER_H

#include <stdbool.h>

/*
 * Check if an option has been seen multiple times and report error
 * Returns true if this is a duplicate (error condition)
 */
bool check_duplicate_option(bool seen, const char *option_name);

/*
 * Parse an integer argument and validate it
 * Returns true on success, false on error
 */
bool parse_int_argument(const char *str, int *result, const char *option_name);

/*
 * Validate that a required argument is provided
 * Returns true if valid, false if missing
 */
bool validate_required_option(const char *value, const char *option_name);

/*
 * Check if exactly one of two options is provided
 * Returns true if valid, false if both or neither are provided
 */
bool validate_exclusive_options(bool opt1_set, bool opt2_set,
                                 const char *opt1_name, const char *opt2_name);

#endif /* ARG_PARSER_H */

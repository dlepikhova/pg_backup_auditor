/*
 * test_helpers.h
 *
 * Checked wrappers for fixture setup in unit tests: a truncated path or a
 * failed shell command must fail the run instead of passing unnoticed.
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

#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <stdio.h>
#include <stdlib.h>

static inline void
test_require(int ok, const char *what, const char *detail,
			 const char *file, int line)
{
	if (!ok)
	{
		fprintf(stderr, "%s:%d: test setup failed: %s%s%s\n",
				file, line, what, detail ? ": " : "", detail ? detail : "");
		abort();
	}
}

/* snprintf into a char array; aborts if the result does not fit */
#define TEST_SNPRINTF(buf, ...) \
	do { \
		int n_ = snprintf((buf), sizeof(buf), __VA_ARGS__); \
		test_require(n_ >= 0 && (size_t) n_ < sizeof(buf), \
					 "truncated", #buf, __FILE__, __LINE__); \
	} while (0)

/* Run a shell command; aborts unless it exits with 0 */
#define TEST_SYSTEM(cmd) \
	do { \
		const char *cmd_ = (cmd); \
		test_require(system(cmd_) == 0, "command failed", cmd_, \
					 __FILE__, __LINE__); \
	} while (0)

#endif /* TEST_HELPERS_H */

/*
 * test_arg_parser.c
 *
 * Unit tests for argument parsing helpers.
 *
 * Copyright (C) 2026 Daria Lepikhova
 *
 * Licensed under the GNU GPL v3.0 or later.
 */

#include <check.h>
#include <stdlib.h>
#include <string.h>

#include "arg_parser.h"

/* ----- parse_string_option ----- */

/* First call assigns the value and marks the flag. */
START_TEST(test_parse_string_option_first_call)
{
	char *target = NULL;
	bool  seen   = false;
	const char *value = "/some/path";

	ck_assert(parse_string_option("--backup-dir", value, &target, &seen));
	ck_assert_ptr_eq(target, (void *)value);
	ck_assert(seen);
}
END_TEST

/* Second call with the same flag is a duplicate and must fail. */
START_TEST(test_parse_string_option_duplicate)
{
	char *target = NULL;
	bool  seen   = false;
	const char *first  = "/first";
	const char *second = "/second";

	ck_assert(parse_string_option("--backup-dir", first, &target, &seen));
	ck_assert(!parse_string_option("--backup-dir", second, &target, &seen));
	/* Original value preserved on duplicate */
	ck_assert_ptr_eq(target, (void *)first);
}
END_TEST

/* Independent flags do not interfere with each other. */
START_TEST(test_parse_string_option_independent_flags)
{
	char *dir = NULL, *wal = NULL;
	bool  dir_seen = false, wal_seen = false;

	ck_assert(parse_string_option("--backup-dir",  "/d", &dir, &dir_seen));
	ck_assert(parse_string_option("--wal-archive", "/w", &wal, &wal_seen));
	ck_assert_str_eq(dir, "/d");
	ck_assert_str_eq(wal, "/w");
}
END_TEST

Suite *
arg_parser_suite(void)
{
	Suite *s = suite_create("arg_parser");

	TCase *tc = tcase_create("parse_string_option");
	tcase_add_test(tc, test_parse_string_option_first_call);
	tcase_add_test(tc, test_parse_string_option_duplicate);
	tcase_add_test(tc, test_parse_string_option_independent_flags);
	suite_add_tcase(s, tc);

	return s;
}

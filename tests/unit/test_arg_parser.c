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
#include <time.h>

#include "arg_parser.h"

/* Helper: build expected time_t for a DD-MM-YYYY at local midnight,
 * using the same struct tm shape the parser uses. Lets test assertions
 * be TZ-agnostic — we compare against what mktime would produce anyway. */
static time_t
expected_local_midnight(int day, int month, int year)
{
	struct tm tm = {
		.tm_mday  = day,
		.tm_mon   = month - 1,
		.tm_year  = year - 1900,
		.tm_isdst = -1,
	};
	return mktime(&tm);
}

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

/* ----- parse_date_option ----- */

/* Valid date parses to local midnight of that day. */
START_TEST(test_parse_date_option_valid)
{
	time_t out = 0;
	bool   seen = false;

	ck_assert(parse_date_option("--since", "15-08-2026", &out, &seen));
	ck_assert(seen);
	ck_assert_int_eq(out, expected_local_midnight(15, 8, 2026));
}
END_TEST

/* Boundary years accepted (1970 and 2100). */
START_TEST(test_parse_date_option_boundary_years)
{
	time_t out = 0;
	bool   seen = false;

	ck_assert(parse_date_option("--since", "01-01-1970", &out, &seen));
	seen = false;
	ck_assert(parse_date_option("--before", "31-12-2100", &out, &seen));
}
END_TEST

/* Leap-year Feb 29 in a leap year is valid; in a non-leap year — invalid. */
START_TEST(test_parse_date_option_leap_year)
{
	time_t out = 0;
	bool   seen = false;

	ck_assert(parse_date_option("--since", "29-02-2024", &out, &seen));
	seen = false;
	ck_assert(!parse_date_option("--since", "29-02-2025", &out, &seen));
}
END_TEST

/* Duplicate invocation returns false and leaves target/seen intact. */
START_TEST(test_parse_date_option_duplicate)
{
	time_t out = 0;
	bool   seen = false;

	ck_assert(parse_date_option("--since", "15-08-2026", &out, &seen));
	time_t first = out;

	ck_assert(!parse_date_option("--since", "16-08-2026", &out, &seen));
	ck_assert_int_eq(out, first);
}
END_TEST

/* Format errors: missing padding, wrong separator, short year, extras,
 * empty string, NULL. All must fail without touching target/seen. */
START_TEST(test_parse_date_option_bad_format)
{
	time_t out;
	bool   seen;
	const char *bad[] = {
		"8-1-2026",      /* not zero-padded */
		"15/08/2026",    /* wrong separator */
		"15-08-26",      /* 2-digit year */
		"15-08-2026x",   /* trailing junk */
		"",              /* empty */
		"aa-bb-cccc",    /* non-digits */
		NULL,            /* null pointer */
	};

	for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++)
	{
		out = 0;
		seen = false;
		ck_assert_msg(!parse_date_option("--since", bad[i], &out, &seen),
					  "expected failure for input #%zu", i);
		ck_assert_int_eq(out, 0);
		ck_assert(!seen);
	}
}
END_TEST

/* Year outside [1970..2100] is rejected. */
START_TEST(test_parse_date_option_year_range)
{
	time_t out = 0;
	bool   seen = false;

	ck_assert(!parse_date_option("--since", "01-01-1969", &out, &seen));
	ck_assert(!parse_date_option("--since", "01-01-2101", &out, &seen));
}
END_TEST

/* Format-valid but calendar-invalid dates (mktime normalizes silently;
 * the parser must detect the drift and reject). */
START_TEST(test_parse_date_option_invalid_calendar)
{
	time_t out = 0;
	bool   seen = false;

	ck_assert(!parse_date_option("--since", "31-02-2026", &out, &seen));
	ck_assert(!parse_date_option("--since", "31-04-2026", &out, &seen));  /* April has 30 */
	ck_assert(!parse_date_option("--since", "00-01-2026", &out, &seen));  /* day 0 */
	ck_assert(!parse_date_option("--since", "01-00-2026", &out, &seen));  /* month 0 */
	ck_assert(!parse_date_option("--since", "01-13-2026", &out, &seen));  /* month 13 */
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

	TCase *tc_date = tcase_create("parse_date_option");
	tcase_add_test(tc_date, test_parse_date_option_valid);
	tcase_add_test(tc_date, test_parse_date_option_boundary_years);
	tcase_add_test(tc_date, test_parse_date_option_leap_year);
	tcase_add_test(tc_date, test_parse_date_option_duplicate);
	tcase_add_test(tc_date, test_parse_date_option_bad_format);
	tcase_add_test(tc_date, test_parse_date_option_year_range);
	tcase_add_test(tc_date, test_parse_date_option_invalid_calendar);
	suite_add_tcase(s, tc_date);

	return s;
}

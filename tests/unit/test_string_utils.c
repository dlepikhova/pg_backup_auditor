/*
 * test_string_utils.c
 *
 * Unit tests for string utilities
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


#include <check.h>
#include <stdlib.h>
#include <string.h>
#include "pg_backup_auditor.h"

/* Test str_trim() with leading whitespace */
START_TEST(test_str_trim_leading)
{
	char str[] = "   hello";
	char *result = str_trim(str);
	ck_assert_str_eq(result, "hello");
}
END_TEST

/* Test str_trim() with trailing whitespace */
START_TEST(test_str_trim_trailing)
{
	char str[] = "hello   ";
	char *result = str_trim(str);
	ck_assert_str_eq(result, "hello");
}
END_TEST

/* Test str_trim() with both leading and trailing whitespace */
START_TEST(test_str_trim_both)
{
	char str[] = "  hello world  ";
	char *result = str_trim(str);
	ck_assert_str_eq(result, "hello world");
}
END_TEST

/* Test str_trim() with only whitespace */
START_TEST(test_str_trim_only_whitespace)
{
	char str[] = "     ";
	char *result = str_trim(str);
	ck_assert_str_eq(result, "");
}
END_TEST

/* Test str_trim() with empty string */
START_TEST(test_str_trim_empty)
{
	char str[] = "";
	char *result = str_trim(str);
	ck_assert_str_eq(result, "");
}
END_TEST

/* Test str_trim() with no whitespace */
START_TEST(test_str_trim_no_whitespace)
{
	char str[] = "hello";
	char *result = str_trim(str);
	ck_assert_str_eq(result, "hello");
}
END_TEST

/* Test str_copy() normal case */
START_TEST(test_str_copy_normal)
{
	char dest[20];
	str_copy(dest, "hello", sizeof(dest));
	ck_assert_str_eq(dest, "hello");
}
END_TEST

/* Test str_copy() with truncation */
START_TEST(test_str_copy_truncate)
{
	char dest[5];
	str_copy(dest, "hello world", sizeof(dest));
	ck_assert_str_eq(dest, "hell");
	ck_assert_int_eq(strlen(dest), 4);
}
END_TEST

/* Test str_copy() with exact fit */
START_TEST(test_str_copy_exact_fit)
{
	char dest[6];
	str_copy(dest, "hello", sizeof(dest));
	ck_assert_str_eq(dest, "hello");
}
END_TEST

/* Test str_copy() with empty string */
START_TEST(test_str_copy_empty)
{
	char dest[10];
	dest[0] = 'x';  /* Put something there first */
	str_copy(dest, "", sizeof(dest));
	ck_assert_str_eq(dest, "");
}
END_TEST

/* Create test suite for string utilities */
Suite *
string_utils_suite(void)
{
	Suite *s;
	TCase *tc_trim;
	TCase *tc_copy;

	s = suite_create("String Utils");

	/* Test case for str_trim */
	tc_trim = tcase_create("str_trim");
	tcase_add_test(tc_trim, test_str_trim_leading);
	tcase_add_test(tc_trim, test_str_trim_trailing);
	tcase_add_test(tc_trim, test_str_trim_both);
	tcase_add_test(tc_trim, test_str_trim_only_whitespace);
	tcase_add_test(tc_trim, test_str_trim_empty);
	tcase_add_test(tc_trim, test_str_trim_no_whitespace);
	suite_add_tcase(s, tc_trim);

	/* Test case for str_copy */
	tc_copy = tcase_create("str_copy");
	tcase_add_test(tc_copy, test_str_copy_normal);
	tcase_add_test(tc_copy, test_str_copy_truncate);
	tcase_add_test(tc_copy, test_str_copy_exact_fit);
	tcase_add_test(tc_copy, test_str_copy_empty);
	suite_add_tcase(s, tc_copy);

	return s;
}

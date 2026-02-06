/*
 * test_adapter_registry.c
 *
 * Unit tests for adapter registry functions
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

/* Test backup_type_to_string() */
START_TEST(test_backup_type_to_string_full)
{
	const char *result = backup_type_to_string(BACKUP_TYPE_FULL);
	ck_assert_str_eq(result, "FULL");
}
END_TEST

START_TEST(test_backup_type_to_string_page)
{
	const char *result = backup_type_to_string(BACKUP_TYPE_PAGE);
	ck_assert_str_eq(result, "PAGE");
}
END_TEST

START_TEST(test_backup_type_to_string_delta)
{
	const char *result = backup_type_to_string(BACKUP_TYPE_DELTA);
	ck_assert_str_eq(result, "DELTA");
}
END_TEST

START_TEST(test_backup_type_to_string_ptrack)
{
	const char *result = backup_type_to_string(BACKUP_TYPE_PTRACK);
	ck_assert_str_eq(result, "PTRACK");
}
END_TEST

START_TEST(test_backup_type_to_string_unknown)
{
	const char *result = backup_type_to_string((BackupType)999);
	ck_assert_str_eq(result, "UNKNOWN");
}
END_TEST

/* Test backup_tool_to_string() */
START_TEST(test_backup_tool_to_string_basebackup)
{
	const char *result = backup_tool_to_string(BACKUP_TOOL_PG_BASEBACKUP);
	ck_assert_str_eq(result, "pg_basebackup");
}
END_TEST

START_TEST(test_backup_tool_to_string_probackup)
{
	const char *result = backup_tool_to_string(BACKUP_TOOL_PG_PROBACKUP);
	ck_assert_str_eq(result, "pg_probackup");
}
END_TEST

START_TEST(test_backup_tool_to_string_unknown)
{
	const char *result = backup_tool_to_string((BackupTool)999);
	ck_assert_str_eq(result, "unknown");
}
END_TEST

/* Test backup_status_to_string() */
START_TEST(test_backup_status_to_string_ok)
{
	const char *result = backup_status_to_string(BACKUP_STATUS_OK);
	ck_assert_str_eq(result, "OK");
}
END_TEST

START_TEST(test_backup_status_to_string_running)
{
	const char *result = backup_status_to_string(BACKUP_STATUS_RUNNING);
	ck_assert_str_eq(result, "RUNNING");
}
END_TEST

START_TEST(test_backup_status_to_string_corrupt)
{
	const char *result = backup_status_to_string(BACKUP_STATUS_CORRUPT);
	ck_assert_str_eq(result, "CORRUPT");
}
END_TEST

START_TEST(test_backup_status_to_string_error)
{
	const char *result = backup_status_to_string(BACKUP_STATUS_ERROR);
	ck_assert_str_eq(result, "ERROR");
}
END_TEST

START_TEST(test_backup_status_to_string_orphan)
{
	const char *result = backup_status_to_string(BACKUP_STATUS_ORPHAN);
	ck_assert_str_eq(result, "ORPHAN");
}
END_TEST

START_TEST(test_backup_status_to_string_warning)
{
	const char *result = backup_status_to_string(BACKUP_STATUS_WARNING);
	ck_assert_str_eq(result, "WARNING");
}
END_TEST

START_TEST(test_backup_status_to_string_unknown)
{
	const char *result = backup_status_to_string((BackupStatus)999);
	ck_assert_str_eq(result, "UNKNOWN");
}
END_TEST

/* Create test suite for adapter registry */
Suite *
adapter_registry_suite(void)
{
	Suite *s;
	TCase *tc_type;
	TCase *tc_tool;
	TCase *tc_status;

	s = suite_create("Adapter Registry");

	/* Test case for backup_type_to_string */
	tc_type = tcase_create("backup_type_to_string");
	tcase_add_test(tc_type, test_backup_type_to_string_full);
	tcase_add_test(tc_type, test_backup_type_to_string_page);
	tcase_add_test(tc_type, test_backup_type_to_string_delta);
	tcase_add_test(tc_type, test_backup_type_to_string_ptrack);
	tcase_add_test(tc_type, test_backup_type_to_string_unknown);
	suite_add_tcase(s, tc_type);

	/* Test case for backup_tool_to_string */
	tc_tool = tcase_create("backup_tool_to_string");
	tcase_add_test(tc_tool, test_backup_tool_to_string_basebackup);
	tcase_add_test(tc_tool, test_backup_tool_to_string_probackup);
	tcase_add_test(tc_tool, test_backup_tool_to_string_unknown);
	suite_add_tcase(s, tc_tool);

	/* Test case for backup_status_to_string */
	tc_status = tcase_create("backup_status_to_string");
	tcase_add_test(tc_status, test_backup_status_to_string_ok);
	tcase_add_test(tc_status, test_backup_status_to_string_running);
	tcase_add_test(tc_status, test_backup_status_to_string_corrupt);
	tcase_add_test(tc_status, test_backup_status_to_string_error);
	tcase_add_test(tc_status, test_backup_status_to_string_orphan);
	tcase_add_test(tc_status, test_backup_status_to_string_warning);
	tcase_add_test(tc_status, test_backup_status_to_string_unknown);
	suite_add_tcase(s, tc_status);

	return s;
}

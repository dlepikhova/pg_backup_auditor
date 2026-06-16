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

/* ----- backup_tool_from_string ----- */

START_TEST(test_backup_tool_from_string_known)
{
	BackupTool t;
	ck_assert(backup_tool_from_string("pg_basebackup", &t));
	ck_assert_int_eq(t, BACKUP_TOOL_PG_BASEBACKUP);
	ck_assert(backup_tool_from_string("pg_probackup", &t));
	ck_assert_int_eq(t, BACKUP_TOOL_PG_PROBACKUP);
	ck_assert(backup_tool_from_string("pgbackrest", &t));
	ck_assert_int_eq(t, BACKUP_TOOL_PGBACKREST);
}
END_TEST

START_TEST(test_backup_tool_from_string_case_insensitive)
{
	BackupTool t;
	ck_assert(backup_tool_from_string("PG_BASEBACKUP", &t));
	ck_assert_int_eq(t, BACKUP_TOOL_PG_BASEBACKUP);
	ck_assert(backup_tool_from_string("PgBackRest", &t));
	ck_assert_int_eq(t, BACKUP_TOOL_PGBACKREST);
}
END_TEST

START_TEST(test_backup_tool_from_string_invalid)
{
	BackupTool t = BACKUP_TOOL_PG_PROBACKUP;  /* sentinel */
	ck_assert(!backup_tool_from_string("nope", &t));
	ck_assert(!backup_tool_from_string("", &t));
	ck_assert(!backup_tool_from_string(NULL, &t));
	ck_assert(!backup_tool_from_string("pg_basebackup", NULL));
	/* sentinel must be untouched on failure */
	ck_assert_int_eq(t, BACKUP_TOOL_PG_PROBACKUP);
}
END_TEST

/* ----- backup_status_from_string ----- */

START_TEST(test_backup_status_from_string_known)
{
	BackupStatus st;
	ck_assert(backup_status_from_string("ok",      &st));
	ck_assert_int_eq(st, BACKUP_STATUS_OK);
	ck_assert(backup_status_from_string("running", &st));
	ck_assert_int_eq(st, BACKUP_STATUS_RUNNING);
	ck_assert(backup_status_from_string("corrupt", &st));
	ck_assert_int_eq(st, BACKUP_STATUS_CORRUPT);
	ck_assert(backup_status_from_string("error",   &st));
	ck_assert_int_eq(st, BACKUP_STATUS_ERROR);
	ck_assert(backup_status_from_string("orphan",  &st));
	ck_assert_int_eq(st, BACKUP_STATUS_ORPHAN);
	ck_assert(backup_status_from_string("warning", &st));
	ck_assert_int_eq(st, BACKUP_STATUS_WARNING);
}
END_TEST

START_TEST(test_backup_status_from_string_case_insensitive)
{
	BackupStatus st;
	ck_assert(backup_status_from_string("OK", &st));
	ck_assert_int_eq(st, BACKUP_STATUS_OK);
	ck_assert(backup_status_from_string("Warning", &st));
	ck_assert_int_eq(st, BACKUP_STATUS_WARNING);
}
END_TEST

START_TEST(test_backup_status_from_string_invalid)
{
	BackupStatus st = BACKUP_STATUS_OK;
	ck_assert(!backup_status_from_string("done",  &st));
	ck_assert(!backup_status_from_string("",      &st));
	ck_assert(!backup_status_from_string(NULL,    &st));
	ck_assert(!backup_status_from_string("ok",    NULL));
	ck_assert_int_eq(st, BACKUP_STATUS_OK);
}
END_TEST

/* ----- validation_level_from_string ----- */

START_TEST(test_validation_level_from_string_known)
{
	ValidationLevel lvl;
	ck_assert(validation_level_from_string("basic",     &lvl));
	ck_assert_int_eq(lvl, VALIDATION_LEVEL_BASIC);
	ck_assert(validation_level_from_string("standard",  &lvl));
	ck_assert_int_eq(lvl, VALIDATION_LEVEL_STANDARD);
	ck_assert(validation_level_from_string("checksums", &lvl));
	ck_assert_int_eq(lvl, VALIDATION_LEVEL_CHECKSUMS);
	ck_assert(validation_level_from_string("full",      &lvl));
	ck_assert_int_eq(lvl, VALIDATION_LEVEL_FULL);
}
END_TEST

START_TEST(test_validation_level_from_string_case_insensitive)
{
	ValidationLevel lvl;
	ck_assert(validation_level_from_string("BASIC", &lvl));
	ck_assert_int_eq(lvl, VALIDATION_LEVEL_BASIC);
	ck_assert(validation_level_from_string("Full",  &lvl));
	ck_assert_int_eq(lvl, VALIDATION_LEVEL_FULL);
}
END_TEST

START_TEST(test_validation_level_from_string_invalid)
{
	ValidationLevel lvl = VALIDATION_LEVEL_BASIC;
	ck_assert(!validation_level_from_string("paranoid", &lvl));
	ck_assert(!validation_level_from_string("",         &lvl));
	ck_assert(!validation_level_from_string(NULL,       &lvl));
	ck_assert(!validation_level_from_string("basic",    NULL));
	ck_assert_int_eq(lvl, VALIDATION_LEVEL_BASIC);
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

	/* Test case for *_from_string parsers */
	{
		TCase *tc_from = tcase_create("from_string");
		tcase_add_test(tc_from, test_backup_tool_from_string_known);
		tcase_add_test(tc_from, test_backup_tool_from_string_case_insensitive);
		tcase_add_test(tc_from, test_backup_tool_from_string_invalid);
		tcase_add_test(tc_from, test_backup_status_from_string_known);
		tcase_add_test(tc_from, test_backup_status_from_string_case_insensitive);
		tcase_add_test(tc_from, test_backup_status_from_string_invalid);
		tcase_add_test(tc_from, test_validation_level_from_string_known);
		tcase_add_test(tc_from, test_validation_level_from_string_case_insensitive);
		tcase_add_test(tc_from, test_validation_level_from_string_invalid);
		suite_add_tcase(s, tc_from);
	}

	return s;
}

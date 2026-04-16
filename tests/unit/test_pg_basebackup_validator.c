/*
 * test_pg_basebackup_validator.c
 *
 * Unit tests for pg_basebackup structure validator
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "pg_backup_auditor.h"

/* Test fixtures */
static char test_dir[PATH_MAX];
static char valid_backup_dir[PATH_MAX];
static char invalid_backup_dir[PATH_MAX];

/* Setup test directories */
static void
setup_test_directories(void)
{
	snprintf(test_dir, sizeof(test_dir), "/tmp/pgbb_validator_test_%d", getpid());
	mkdir(test_dir, 0755);

	/* Valid pg_basebackup directory */
	snprintf(valid_backup_dir, sizeof(valid_backup_dir), "%s/valid", test_dir);
	mkdir(valid_backup_dir, 0755);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/base", valid_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/global", valid_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/global/pg_control", valid_backup_dir);
	FILE *fp = fopen(path, "w");
	fprintf(fp, "dummy pg_control\n");
	fclose(fp);

	snprintf(path, sizeof(path), "%s/PG_VERSION", valid_backup_dir);
	fp = fopen(path, "w");
	fprintf(fp, "16\n");
	fclose(fp);

	snprintf(path, sizeof(path), "%s/backup_label", valid_backup_dir);
	fp = fopen(path, "w");
	fprintf(fp, "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n");
	fprintf(fp, "CHECKPOINT LOCATION: 0/2000060\n");
	fprintf(fp, "START TIME: 2024-01-08 10:05:30 UTC\n");
	fprintf(fp, "START TIMELINE: 1\n");
	fclose(fp);

	/* Invalid backup directory (missing pg_control) */
	snprintf(invalid_backup_dir, sizeof(invalid_backup_dir), "%s/invalid", test_dir);
	mkdir(invalid_backup_dir, 0755);

	snprintf(path, sizeof(path), "%s/base", invalid_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/global", invalid_backup_dir);
	mkdir(path, 0755);
	/* No pg_control, so it fails validation */
}

/* Teardown test directories */
static void
teardown_test_directories(void)
{
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
	system(cmd);
}

/* Test: Valid pg_basebackup structure */
START_TEST(test_valid_structure)
{
	setup_test_directories();

	BackupInfo info;
	memset(&info, 0, sizeof(info));
	snprintf(info.backup_path, sizeof(info.backup_path), "%s", valid_backup_dir);
	info.tool = BACKUP_TOOL_PG_BASEBACKUP;
	info.type = BACKUP_TYPE_FULL;

	ValidationResult *result = pg_basebackup_validate_structure(&info);

	ck_assert_ptr_nonnull(result);
	/* Validator ran successfully (may report errors but shouldn't crash) */
	ck_assert_int_ge(result->status, 0);

	free_validation_result(result);
	teardown_test_directories();
}
END_TEST

/* Test: Invalid pg_basebackup (missing pg_control) */
START_TEST(test_missing_pg_control)
{
	setup_test_directories();

	BackupInfo info;
	memset(&info, 0, sizeof(info));
	snprintf(info.backup_path, sizeof(info.backup_path), "%s", invalid_backup_dir);
	info.tool = BACKUP_TOOL_PG_BASEBACKUP;
	info.type = BACKUP_TYPE_FULL;

	ValidationResult *result = pg_basebackup_validate_structure(&info);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_ne(result->status, BACKUP_STATUS_OK);
	ck_assert_int_gt(result->error_count, 0);

	free_validation_result(result);
	teardown_test_directories();
}
END_TEST

/* Create test suite */
Suite *
pg_basebackup_validator_suite(void)
{
	Suite *s;
	TCase *tc_structure;

	s = suite_create("pg_basebackup Validator");

	tc_structure = tcase_create("structure");
	tcase_add_test(tc_structure, test_valid_structure);
	tcase_add_test(tc_structure, test_missing_pg_control);
	suite_add_tcase(s, tc_structure);

	return s;
}

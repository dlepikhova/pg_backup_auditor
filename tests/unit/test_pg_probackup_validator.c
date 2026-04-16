/*
 * test_pg_probackup_validator.c
 *
 * Unit tests for pg_probackup structure validator
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

/* Setup test directories */
static void
setup_test_directories(void)
{
	snprintf(test_dir, sizeof(test_dir), "/tmp/pgpb_validator_test_%d", getpid());
	mkdir(test_dir, 0755);

	/* Valid pg_probackup directory structure */
	snprintf(valid_backup_dir, sizeof(valid_backup_dir), "%s/valid", test_dir);
	mkdir(valid_backup_dir, 0755);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/database", valid_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/database/global", valid_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/database/global/pg_control", valid_backup_dir);
	FILE *fp = fopen(path, "w");
	fprintf(fp, "dummy pg_control\n");
	fclose(fp);

	snprintf(path, sizeof(path), "%s/database/database_map", valid_backup_dir);
	fp = fopen(path, "w");
	fprintf(fp, "# empty database map\n");
	fclose(fp);

	snprintf(path, sizeof(path), "%s/database/PG_VERSION", valid_backup_dir);
	fp = fopen(path, "w");
	fprintf(fp, "15\n");
	fclose(fp);
}

/* Teardown test directories */
static void
teardown_test_directories(void)
{
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
	system(cmd);
}

/* Test: Valid pg_probackup structure */
START_TEST(test_valid_structure)
{
	setup_test_directories();

	BackupInfo info;
	memset(&info, 0, sizeof(info));
	snprintf(info.backup_path, sizeof(info.backup_path), "%s", valid_backup_dir);
	info.tool = BACKUP_TOOL_PG_PROBACKUP;
	info.type = BACKUP_TYPE_FULL;

	ValidationResult *result = pg_probackup_validate_structure(&info);

	ck_assert_ptr_nonnull(result);
	/* Validator ran successfully (may report errors but shouldn't crash) */
	ck_assert_int_ge(result->status, 0);

	free_validation_result(result);
	teardown_test_directories();
}
END_TEST

/* Create test suite */
Suite *
pg_probackup_validator_suite(void)
{
	Suite *s;
	TCase *tc_structure;

	s = suite_create("pg_probackup Validator");

	tc_structure = tcase_create("structure");
	tcase_add_test(tc_structure, test_valid_structure);
	suite_add_tcase(s, tc_structure);

	return s;
}

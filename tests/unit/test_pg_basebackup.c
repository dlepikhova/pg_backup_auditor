/*
 * test_pg_basebackup.c
 *
 * Unit tests for pg_basebackup adapter
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


#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "pg_backup_auditor.h"

/* Test fixtures - temporary directories for testing */
static char test_dir[PATH_MAX];
static char plain_backup_dir[PATH_MAX];
static char tar_backup_dir[PATH_MAX];
static char invalid_backup_dir[PATH_MAX];

/* Setup: Create test directory structure */
static void
setup_test_directories(void)
{
	/* Create base test directory */
	snprintf(test_dir, sizeof(test_dir), "/tmp/pg_backup_test_%d", getpid());
	mkdir(test_dir, 0755);

	/* Create plain format backup directory */
	snprintf(plain_backup_dir, sizeof(plain_backup_dir), "%s/plain_backup", test_dir);
	mkdir(plain_backup_dir, 0755);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/base", plain_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/global", plain_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/backup_label", plain_backup_dir);
	FILE *fp = fopen(path, "w");
	fprintf(fp, "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n");
	fprintf(fp, "CHECKPOINT LOCATION: 0/2000060\n");
	fprintf(fp, "BACKUP METHOD: streamed\n");
	fprintf(fp, "BACKUP FROM: primary\n");
	fprintf(fp, "START TIME: 2024-01-08 10:05:30 UTC\n");
	fprintf(fp, "LABEL: test backup\n");
	fprintf(fp, "START TIMELINE: 1\n");
	fclose(fp);

	/* Add PG_VERSION file */
	snprintf(path, sizeof(path), "%s/PG_VERSION", plain_backup_dir);
	fp = fopen(path, "w");
	fprintf(fp, "17\n");
	fclose(fp);

	/* Create tar format backup directory */
	snprintf(tar_backup_dir, sizeof(tar_backup_dir), "%s/tar_backup", test_dir);
	mkdir(tar_backup_dir, 0755);

	snprintf(path, sizeof(path), "%s/base.tar.gz", tar_backup_dir);
	fp = fopen(path, "w");
	fprintf(fp, "dummy tar file\n");
	fclose(fp);

	snprintf(path, sizeof(path), "%s/backup_label", tar_backup_dir);
	fp = fopen(path, "w");
	fprintf(fp, "START WAL LOCATION: 0/3000028 (file 000000010000000000000003)\n");
	fprintf(fp, "START TIME: 2024-01-09 14:30:15 UTC\n");
	fprintf(fp, "START TIMELINE: 1\n");
	fclose(fp);

	/* Create invalid backup directory (missing markers) */
	snprintf(invalid_backup_dir, sizeof(invalid_backup_dir), "%s/invalid", test_dir);
	mkdir(invalid_backup_dir, 0755);

	snprintf(path, sizeof(path), "%s/base", invalid_backup_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/global", invalid_backup_dir);
	mkdir(path, 0755);
	/* No backup_label or backup_manifest - invalid */
}

/* Teardown: Remove test directories */
static void
teardown_test_directories(void)
{
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
	system(cmd);
}

/* Test: pg_basebackup_detect() for plain format */
START_TEST(test_detect_plain_format)
{
	setup_test_directories();

	bool result = pg_basebackup_adapter.detect(plain_backup_dir);
	ck_assert_int_eq(result, true);

	teardown_test_directories();
}
END_TEST

/* Test: pg_basebackup_detect() for tar format */
START_TEST(test_detect_tar_format)
{
	setup_test_directories();

	bool result = pg_basebackup_adapter.detect(tar_backup_dir);
	ck_assert_int_eq(result, true);

	teardown_test_directories();
}
END_TEST

/* Test: pg_basebackup_detect() for invalid backup */
START_TEST(test_detect_invalid_backup)
{
	setup_test_directories();

	bool result = pg_basebackup_adapter.detect(invalid_backup_dir);
	ck_assert_int_eq(result, false);

	teardown_test_directories();
}
END_TEST

/* Test: pg_basebackup_detect() for non-existent directory */
START_TEST(test_detect_nonexistent)
{
	bool result = pg_basebackup_adapter.detect("/tmp/nonexistent_backup_dir_12345");
	ck_assert_int_eq(result, false);
}
END_TEST

/* Test: pg_basebackup_scan() for plain format */
START_TEST(test_scan_plain_backup)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->backup_id, "20240108-100530");
	ck_assert_str_eq(info->node_name, "localhost");
	ck_assert_int_eq(info->tool, BACKUP_TOOL_PG_BASEBACKUP);
	ck_assert_int_eq(info->type, BACKUP_TYPE_FULL);
	ck_assert_int_eq(info->status, BACKUP_STATUS_OK);
	ck_assert_int_eq(info->timeline, 1);
	ck_assert(info->start_lsn == 0x2000028ULL);
	ck_assert(info->start_time > 0);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: pg_basebackup_scan() for tar format */
START_TEST(test_scan_tar_backup)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(tar_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->backup_id, "20240109-143015");
	ck_assert_str_eq(info->node_name, "localhost");
	ck_assert_int_eq(info->tool, BACKUP_TOOL_PG_BASEBACKUP);
	ck_assert_int_eq(info->type, BACKUP_TYPE_FULL);
	ck_assert_int_eq(info->status, BACKUP_STATUS_OK);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: pg_basebackup_scan() without backup_label */
START_TEST(test_scan_missing_backup_label)
{
	setup_test_directories();

	/* Remove backup_label from tar backup */
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/backup_label", tar_backup_dir);
	unlink(path);

	BackupInfo *info = pg_basebackup_adapter.scan(tar_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_int_eq(info->status, BACKUP_STATUS_ERROR);
	ck_assert_str_eq(info->backup_id, "tar_backup");  /* Falls back to directory name */

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: backup_id generation from timestamp */
START_TEST(test_backup_id_generation)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	/* Verify backup_id format: YYYYMMDD-HHMMSS */
	ck_assert_int_eq(strlen(info->backup_id), 15);
	ck_assert_int_eq(info->backup_id[8], '-');

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: LSN parsing */
START_TEST(test_lsn_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	/* START WAL LOCATION: 0/2000028 */
	ck_assert_uint_eq(info->start_lsn, 0x2000028ULL);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: timeline parsing */
START_TEST(test_timeline_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_int_eq(info->timeline, 1);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: default node_name is localhost */
START_TEST(test_default_node_name)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->node_name, "localhost");

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: stop_lsn (CHECKPOINT LOCATION) parsing */
START_TEST(test_stop_lsn_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	/* CHECKPOINT LOCATION: 0/2000060 */
	ck_assert_uint_eq(info->stop_lsn, 0x2000060ULL);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: backup_method parsing */
START_TEST(test_backup_method_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->backup_method, "streamed");

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: backup_from parsing */
START_TEST(test_backup_from_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->backup_from, "primary");

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: backup_label parsing */
START_TEST(test_backup_label_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->backup_label, "test backup");

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: WAL start file extraction */
START_TEST(test_wal_start_file_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->wal_start_file, "000000010000000000000002");

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: PG_VERSION parsing */
START_TEST(test_pg_version_parsing)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	/* "17" -> 170000 */
	ck_assert_uint_eq(info->pg_version, 170000);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: PG_VERSION with minor version */
START_TEST(test_pg_version_with_minor)
{
	setup_test_directories();

	/* Override PG_VERSION with minor version */
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/PG_VERSION", plain_backup_dir);
	FILE *fp = fopen(path, "w");
	fprintf(fp, "16.1\n");
	fclose(fp);

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	/* "16.1" -> 160001 */
	ck_assert_uint_eq(info->pg_version, 160001);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: data_bytes calculation */
START_TEST(test_data_bytes_calculation)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	/* Should be > 0 since we created files */
	ck_assert(info->data_bytes > 0);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Test: end_time from directory mtime */
START_TEST(test_end_time_from_mtime)
{
	setup_test_directories();

	BackupInfo *info = pg_basebackup_adapter.scan(plain_backup_dir);

	ck_assert_ptr_nonnull(info);
	/* Should be set to directory modification time */
	ck_assert(info->end_time > 0);

	free(info);
	teardown_test_directories();
}
END_TEST

/* Create test suite for pg_basebackup adapter */
Suite *
pg_basebackup_suite(void)
{
	Suite *s;
	TCase *tc_detect;
	TCase *tc_scan;
	TCase *tc_metadata;

	s = suite_create("pg_basebackup Adapter");

	/* Test case for detection */
	tc_detect = tcase_create("detect");
	tcase_add_test(tc_detect, test_detect_plain_format);
	tcase_add_test(tc_detect, test_detect_tar_format);
	tcase_add_test(tc_detect, test_detect_invalid_backup);
	tcase_add_test(tc_detect, test_detect_nonexistent);
	suite_add_tcase(s, tc_detect);

	/* Test case for scanning */
	tc_scan = tcase_create("scan");
	tcase_add_test(tc_scan, test_scan_plain_backup);
	tcase_add_test(tc_scan, test_scan_tar_backup);
	tcase_add_test(tc_scan, test_scan_missing_backup_label);
	suite_add_tcase(s, tc_scan);

	/* Test case for metadata parsing */
	tc_metadata = tcase_create("metadata");
	tcase_add_test(tc_metadata, test_backup_id_generation);
	tcase_add_test(tc_metadata, test_lsn_parsing);
	tcase_add_test(tc_metadata, test_timeline_parsing);
	tcase_add_test(tc_metadata, test_default_node_name);
	tcase_add_test(tc_metadata, test_stop_lsn_parsing);
	tcase_add_test(tc_metadata, test_backup_method_parsing);
	tcase_add_test(tc_metadata, test_backup_from_parsing);
	tcase_add_test(tc_metadata, test_backup_label_parsing);
	tcase_add_test(tc_metadata, test_wal_start_file_parsing);
	tcase_add_test(tc_metadata, test_pg_version_parsing);
	tcase_add_test(tc_metadata, test_pg_version_with_minor);
	tcase_add_test(tc_metadata, test_data_bytes_calculation);
	tcase_add_test(tc_metadata, test_end_time_from_mtime);
	suite_add_tcase(s, tc_metadata);

	return s;
}

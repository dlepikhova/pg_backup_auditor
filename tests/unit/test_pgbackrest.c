/*
 * test_pgbackrest.c
 *
 * Unit tests for pgBackRest adapter
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

/* Test fixtures - temporary directories for testing */
static char test_dir[PATH_MAX];
static char repo_dir[PATH_MAX];
static char stanza_dir[PATH_MAX];

/* Setup: Create test pgBackRest repository structure */
static void
setup_pgbackrest_repo(void)
{
	char path[PATH_MAX];
	FILE *fp;

	/* Create base test directory */
	snprintf(test_dir, sizeof(test_dir), "/tmp/pgbackrest_test_%d", getpid());
	mkdir(test_dir, 0755);

	/* Create repository structure: repo/backup/stanza_name/ */
	snprintf(repo_dir, sizeof(repo_dir), "%s/repo", test_dir);
	mkdir(repo_dir, 0755);

	snprintf(path, sizeof(path), "%s/backup", repo_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/archive", repo_dir);
	mkdir(path, 0755);

	snprintf(stanza_dir, sizeof(stanza_dir), "%s/backup/test_stanza", repo_dir);
	mkdir(stanza_dir, 0755);

	/* Create backup.info file with JSON backup entries */
	snprintf(path, sizeof(path), "%s/backup.info", stanza_dir);
	fp = fopen(path, "w");
	fprintf(fp, "[backup:current]\n");
	fprintf(fp, "20240108-100530F={\"backup-label\":\"20240108-100530F\",");
	fprintf(fp, "\"backup-type\":\"full\",");
	fprintf(fp, "\"backup-timestamp-start\":1704709530,");
	fprintf(fp, "\"backup-timestamp-stop\":1704709650,");
	fprintf(fp, "\"backup-lsn-start\":\"0/2000028\",");
	fprintf(fp, "\"backup-lsn-stop\":\"0/2000060\",");
	fprintf(fp, "\"backup-size\":12345678,");
	fprintf(fp, "\"backup-repo-size\":5678901}\n");
	fprintf(fp, "20240109-143015I={\"backup-label\":\"20240109-143015I\",");
	fprintf(fp, "\"backup-type\":\"incr\",");
	fprintf(fp, "\"backup-timestamp-start\":1704810615,");
	fprintf(fp, "\"backup-timestamp-stop\":1704810715,");
	fprintf(fp, "\"backup-lsn-start\":\"0/3000028\",");
	fprintf(fp, "\"backup-lsn-stop\":\"0/3000128\",");
	fprintf(fp, "\"backup-size\":2345678,");
	fprintf(fp, "\"backup-repo-size\":1234567}\n");
	fclose(fp);

	/* Create backup directories */
	snprintf(path, sizeof(path), "%s/20240108-100530F", stanza_dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/20240109-143015I", stanza_dir);
	mkdir(path, 0755);

	/* Create backup.manifest for full backup */
	snprintf(path, sizeof(path), "%s/20240108-100530F/backup.manifest", stanza_dir);
	fp = fopen(path, "w");
	fprintf(fp, "[backup]\n");
	fprintf(fp, "backup-label=20240108-100530F\n");
	fprintf(fp, "backup-type=full\n");
	fprintf(fp, "backup-timestamp-start=1704709530\n");
	fprintf(fp, "backup-timestamp-stop=1704709650\n");
	fprintf(fp, "backup-lsn-start=0/2000028\n");
	fprintf(fp, "backup-lsn-stop=0/2000060\n");
	fprintf(fp, "backup-size=12345678\n");
	fprintf(fp, "backup-repo-size=5678901\n");
	fprintf(fp, "\n[backup:db]\n");
	fprintf(fp, "db-version=17\n");
	fclose(fp);

	/* Create backup.manifest for incremental backup */
	snprintf(path, sizeof(path), "%s/20240109-143015I/backup.manifest", stanza_dir);
	fp = fopen(path, "w");
	fprintf(fp, "[backup]\n");
	fprintf(fp, "backup-label=20240109-143015I\n");
	fprintf(fp, "backup-type=incr\n");
	fprintf(fp, "backup-timestamp-start=1704810615\n");
	fprintf(fp, "backup-timestamp-stop=1704810715\n");
	fprintf(fp, "backup-lsn-start=0/3000028\n");
	fprintf(fp, "backup-lsn-stop=0/3000128\n");
	fprintf(fp, "backup-size=2345678\n");
	fprintf(fp, "backup-repo-size=1234567\n");
	fprintf(fp, "\n[backup:db]\n");
	fprintf(fp, "db-version=17\n");
	fclose(fp);
}

/* Teardown: Remove test directories */
static void
teardown_pgbackrest_repo(void)
{
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
	system(cmd);
}

/* Test: pgBackRest repository detection */
START_TEST(test_detect_pgbackrest_repo)
{
	setup_pgbackrest_repo();

	bool result = pgbackrest_adapter.detect(repo_dir);
	ck_assert_int_eq(result, true);

	teardown_pgbackrest_repo();
}
END_TEST

/* Test: pgBackRest repository detection - missing backup dir */
START_TEST(test_detect_missing_backup_dir)
{
	setup_pgbackrest_repo();

	/* Remove backup directory */
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "rm -rf %s/backup", repo_dir);
	system(path);

	bool result = pgbackrest_adapter.detect(repo_dir);
	ck_assert_int_eq(result, false);

	teardown_pgbackrest_repo();
}
END_TEST

/* Test: pgBackRest repository detection - missing archive dir */
START_TEST(test_detect_missing_archive_dir)
{
	setup_pgbackrest_repo();

	/* Remove archive directory */
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "rm -rf %s/archive", repo_dir);
	system(path);

	bool result = pgbackrest_adapter.detect(repo_dir);
	ck_assert_int_eq(result, false);

	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Scan pgBackRest repository */
START_TEST(test_scan_pgbackrest_repo)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_int_eq(info->tool, BACKUP_TOOL_PGBACKREST);

	/* Check first backup (full) */
	ck_assert_str_eq(info->backup_id, "20240108-100530F");
	ck_assert_int_eq(info->type, BACKUP_TYPE_FULL);
	ck_assert_str_eq(info->instance_name, "test_stanza");

	/* Check second backup exists (incremental) */
	ck_assert_ptr_nonnull(info->next);
	ck_assert_str_eq(info->next->backup_id, "20240109-143015I");
	ck_assert_int_eq(info->next->type, BACKUP_TYPE_INCREMENTAL);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Parse backup size from backup.info (JSON) */
START_TEST(test_parse_backup_size_from_info)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* Check full backup size */
	ck_assert_uint_eq(info->data_bytes, 12345678);

	/* Check incremental backup size */
	ck_assert_ptr_nonnull(info->next);
	ck_assert_uint_eq(info->next->data_bytes, 2345678);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Parse backup size from backup.manifest (INI) */
START_TEST(test_parse_backup_size_from_manifest)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* Manifest should override or confirm the size from backup.info */
	ck_assert_uint_eq(info->data_bytes, 12345678);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Parse LSN values */
START_TEST(test_parse_lsn_values)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* Check full backup LSN */
	ck_assert_uint_eq(info->start_lsn, 0x2000028ULL);
	ck_assert_uint_eq(info->stop_lsn, 0x2000060ULL);

	/* Check incremental backup LSN */
	ck_assert_ptr_nonnull(info->next);
	ck_assert_uint_eq(info->next->start_lsn, 0x3000028ULL);
	ck_assert_uint_eq(info->next->stop_lsn, 0x3000128ULL);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Parse timestamps */
START_TEST(test_parse_timestamps)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* Check full backup timestamps */
	ck_assert_uint_eq(info->start_time, 1704709530);
	ck_assert_uint_eq(info->end_time, 1704709650);

	/* Check incremental backup timestamps */
	ck_assert_ptr_nonnull(info->next);
	ck_assert_uint_eq(info->next->start_time, 1704810615);
	ck_assert_uint_eq(info->next->end_time, 1704810715);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Parse PostgreSQL version */
START_TEST(test_parse_pg_version)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* Version should be parsed from manifest: 17 -> 170000 */
	ck_assert_uint_eq(info->pg_version, 170000);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Multiple backups in list */
START_TEST(test_multiple_backups)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* Count backups */
	int count = 0;
	BackupInfo *current = info;
	while (current != NULL)
	{
		count++;
		current = current->next;
	}

	ck_assert_int_eq(count, 2);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Backup path construction */
START_TEST(test_backup_path_construction)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* Check backup path includes stanza and backup ID */
	ck_assert(strstr(info->backup_path, "test_stanza") != NULL);
	ck_assert(strstr(info->backup_path, "20240108-100530F") != NULL);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Test: Backup type parsing (full/incr/diff) */
START_TEST(test_backup_type_parsing)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);

	/* First backup is full */
	ck_assert_int_eq(info->type, BACKUP_TYPE_FULL);

	/* Second backup is incremental */
	ck_assert_ptr_nonnull(info->next);
	ck_assert_int_eq(info->next->type, BACKUP_TYPE_INCREMENTAL);

	free_backup_list(info);
	teardown_pgbackrest_repo();
}
END_TEST

/* Create test suite for pgBackRest adapter */
Suite *
pgbackrest_suite(void)
{
	Suite *s;
	TCase *tc_detect;
	TCase *tc_scan;
	TCase *tc_metadata;

	s = suite_create("pgBackRest Adapter");

	/* Test case for detection */
	tc_detect = tcase_create("detect");
	tcase_add_test(tc_detect, test_detect_pgbackrest_repo);
	tcase_add_test(tc_detect, test_detect_missing_backup_dir);
	tcase_add_test(tc_detect, test_detect_missing_archive_dir);
	suite_add_tcase(s, tc_detect);

	/* Test case for scanning */
	tc_scan = tcase_create("scan");
	tcase_add_test(tc_scan, test_scan_pgbackrest_repo);
	tcase_add_test(tc_scan, test_multiple_backups);
	tcase_add_test(tc_scan, test_backup_path_construction);
	suite_add_tcase(s, tc_scan);

	/* Test case for metadata parsing */
	tc_metadata = tcase_create("metadata");
	tcase_add_test(tc_metadata, test_parse_backup_size_from_info);
	tcase_add_test(tc_metadata, test_parse_backup_size_from_manifest);
	tcase_add_test(tc_metadata, test_parse_lsn_values);
	tcase_add_test(tc_metadata, test_parse_timestamps);
	tcase_add_test(tc_metadata, test_parse_pg_version);
	tcase_add_test(tc_metadata, test_backup_type_parsing);
	suite_add_tcase(s, tc_metadata);

	return s;
}

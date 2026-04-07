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
	fprintf(fp, "[backrest]\n");
	fprintf(fp, "backrest-format=5\n");
	fprintf(fp, "backrest-version=\"2.51\"\n\n");
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

/* Test: tool_version parsed from [backrest] section of backup.info */
START_TEST(test_parse_tool_version)
{
	setup_pgbackrest_repo();

	BackupInfo *info = pgbackrest_adapter.scan(repo_dir);

	ck_assert_ptr_nonnull(info);
	ck_assert_str_eq(info->tool_version, "2.51");

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

/* ------------------------------------------------------------------ *
 * pgbackrest_validate_structure tests
 * ------------------------------------------------------------------ */

/* Helper: allocate a minimal BackupInfo pointing at a temp directory */
static BackupInfo *
make_pgbackrest_backup_info(const char *backup_path)
{
	BackupInfo *bi = calloc(1, sizeof(BackupInfo));
	if (bi == NULL)
		return NULL;
	bi->tool = BACKUP_TOOL_PGBACKREST;
	strncpy(bi->backup_path, backup_path, PATH_MAX - 1);
	return bi;
}

/* Helper: create an empty file */
static void
touch_file(const char *path)
{
	FILE *fp = fopen(path, "w");
	if (fp != NULL)
		fclose(fp);
}

START_TEST(test_validate_struct_null_input)
{
	ValidationResult *r = pgbackrest_validate_structure(NULL);
	ck_assert_ptr_null(r);
}
END_TEST

/* Helper: create a full plain pgbackrest backup structure */
static void
make_pgbackrest_plain_structure(const char *dir)
{
	char path[PATH_MAX];

	snprintf(path, sizeof(path), "%s/backup.manifest", dir);
	touch_file(path);

	snprintf(path, sizeof(path), "%s/backup.manifest.copy", dir);
	touch_file(path);

	snprintf(path, sizeof(path), "%s/pg_data", dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/pg_data/global", dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/pg_data/global/pg_control", dir);
	touch_file(path);

	snprintf(path, sizeof(path), "%s/pg_data/PG_VERSION", dir);
	touch_file(path);
}

/* Full structure with manifest.copy → 0 errors, 0 warnings */
START_TEST(test_validate_struct_ok)
{
	char dir[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pgbackrest_vs_%d", getpid());
	mkdir(dir, 0755);

	make_pgbackrest_plain_structure(dir);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_validate_structure(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	ck_assert_int_eq(r->warning_count, 0);
	ck_assert_int_eq(r->status, BACKUP_STATUS_OK);

	free_validation_result(r);
	free(bi);

	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* No manifest.copy → 1 warning */
START_TEST(test_validate_struct_ok_without_copy)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pgbackrest_vsc_%d", getpid());
	mkdir(dir, 0755);

	make_pgbackrest_plain_structure(dir);

	/* Remove the copy */
	snprintf(path, sizeof(path), "%s/backup.manifest.copy", dir);
	unlink(path);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_validate_structure(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	ck_assert_int_ge(r->warning_count, 1);
	ck_assert_int_eq(r->status, BACKUP_STATUS_WARNING);

	free_validation_result(r);
	free(bi);

	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* Missing pg_data/global/pg_control → warning mentioning "pg_control" */
START_TEST(test_validate_struct_missing_pg_control)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pgbackrest_vspc_%d", getpid());
	mkdir(dir, 0755);

	make_pgbackrest_plain_structure(dir);
	snprintf(path, sizeof(path), "%s/pg_data/global/pg_control", dir);
	unlink(path);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_validate_structure(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	ck_assert_int_ge(r->warning_count, 1);
	bool found = false;
	for (int i = 0; i < r->warning_count; i++)
		if (strstr(r->warnings[i], "pg_control")) found = true;
	ck_assert_msg(found, "expected warning about missing pg_control");

	free_validation_result(r);
	free(bi);

	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* Missing pg_data/PG_VERSION → warning mentioning "PG_VERSION" */
START_TEST(test_validate_struct_missing_pg_version)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pgbackrest_vspv_%d", getpid());
	mkdir(dir, 0755);

	make_pgbackrest_plain_structure(dir);
	snprintf(path, sizeof(path), "%s/pg_data/PG_VERSION", dir);
	unlink(path);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_validate_structure(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	ck_assert_int_ge(r->warning_count, 1);
	bool found = false;
	for (int i = 0; i < r->warning_count; i++)
		if (strstr(r->warnings[i], "PG_VERSION")) found = true;
	ck_assert_msg(found, "expected warning about missing PG_VERSION");

	free_validation_result(r);
	free(bi);

	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

START_TEST(test_validate_struct_missing_manifest)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pgbackrest_vsm_%d", getpid());
	mkdir(dir, 0755);

	/* pg_data/ present but no backup.manifest */
	snprintf(path, sizeof(path), "%s/pg_data", dir);
	mkdir(path, 0755);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_validate_structure(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert_int_eq(r->status, BACKUP_STATUS_ERROR);

	free_validation_result(r);
	free(bi);

	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

START_TEST(test_validate_struct_missing_pg_data)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pgbackrest_vsd_%d", getpid());
	mkdir(dir, 0755);

	/* backup.manifest present but no pg_data */
	snprintf(path, sizeof(path), "%s/backup.manifest", dir);
	touch_file(path);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_validate_structure(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert_int_eq(r->status, BACKUP_STATUS_ERROR);

	free_validation_result(r);
	free(bi);

	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* backup-error=y in manifest → error */
START_TEST(test_validate_struct_backup_error)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pgbackrest_vsbe_%d", getpid());
	mkdir(dir, 0755);

	make_pgbackrest_plain_structure(dir);

	/* Overwrite backup.manifest with backup-error=y */
	snprintf(path, sizeof(path), "%s/backup.manifest", dir);
	FILE *fp = fopen(path, "w");
	fprintf(fp, "[backup]\nbackup-label=test\nbackup-error=y\n\n[backup:db]\ndb-version=17\n");
	fclose(fp);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_validate_structure(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_ge(r->error_count, 1);
	bool found = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "backup-error=y")) found = true;
	ck_assert_msg(found, "expected backup-error=y error");
	ck_assert_int_eq(r->status, BACKUP_STATUS_ERROR);

	free_validation_result(r);
	free(bi);

	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* ------------------------------------------------------------------ *
 * pgbackrest_check_manifest_checksums tests
 * ------------------------------------------------------------------ */

/* Compute SHA1 of a small string - used to build valid manifest entries */
static void
sha1_hex_of_string(const char *content, size_t len, char *out_hex)
{
	static const char * const fmts[] = {
		"shasum -a 1 -- '%s' 2>/dev/null",
		"sha1sum -- '%s' 2>/dev/null",
		NULL
	};
	char  tmp_path[PATH_MAX];
	FILE *fp;
	int   i;

	strcpy(out_hex, "0000000000000000000000000000000000000000");

	snprintf(tmp_path, sizeof(tmp_path), "/tmp/pbr_sha1_%d", getpid());
	fp = fopen(tmp_path, "wb");
	if (fp == NULL)
		return;
	fwrite(content, 1, len, fp);
	fclose(fp);

	for (i = 0; fmts[i] != NULL; i++)
	{
		char  cmd[PATH_MAX + 32];
		FILE *pp;
		char  line[256];
		size_t hlen;

		snprintf(cmd, sizeof(cmd), fmts[i], tmp_path);
		pp = popen(cmd, "r");
		if (pp == NULL)
			continue;

		line[0] = '\0';
		fgets(line, sizeof(line), pp);
		pclose(pp);

		hlen = 0;
		while (hlen < 40 && line[hlen] != '\0' &&
			   ((line[hlen] >= '0' && line[hlen] <= '9') ||
				(line[hlen] >= 'a' && line[hlen] <= 'f') ||
				(line[hlen] >= 'A' && line[hlen] <= 'F')))
			hlen++;

		if (hlen == 40)
		{
			memcpy(out_hex, line, 40);
			out_hex[40] = '\0';
			break;
		}
	}

	unlink(tmp_path);
}

/* NULL / not pgbackrest → NULL */
START_TEST(test_pbr_checksums_null_input)
{
	ck_assert_ptr_null(pgbackrest_check_manifest_checksums(NULL));
}
END_TEST

/* No backup.manifest → NULL (not applicable) */
START_TEST(test_pbr_checksums_no_manifest)
{
	char dir[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pbr_ck_nomnf_%d", getpid());
	mkdir(dir, 0755);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_check_manifest_checksums(bi);
	ck_assert_ptr_null(r);

	free(bi);
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* Compressed backup (no pg_data/ dir) → NULL */
START_TEST(test_pbr_checksums_compressed)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pbr_ck_cmp_%d", getpid());
	mkdir(dir, 0755);

	/* manifest exists but pg_data/ is absent (compressed backup) */
	snprintf(path, sizeof(path), "%s/backup.manifest", dir);
	touch_file(path);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_check_manifest_checksums(bi);
	ck_assert_ptr_null(r);

	free(bi);
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* All checksums correct → 0 errors */
START_TEST(test_pbr_checksums_all_ok)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pbr_ck_ok_%d", getpid());
	mkdir(dir, 0755);

	/* Create pg_data/ with one file */
	snprintf(path, sizeof(path), "%s/pg_data", dir);
	mkdir(path, 0755);

	const char *content = "17\n";
	snprintf(path, sizeof(path), "%s/pg_data/PG_VERSION", dir);
	FILE *fp = fopen(path, "w");
	fputs(content, fp);
	fclose(fp);

	/* Compute correct SHA1 */
	char sha1[41];
	sha1_hex_of_string(content, strlen(content), sha1);

	/* Write manifest with correct checksum */
	snprintf(path, sizeof(path), "%s/backup.manifest", dir);
	fp = fopen(path, "w");
	fprintf(fp, "[backup]\nbackup-label=test\n\n");
	fprintf(fp, "[target:file]\n");
	fprintf(fp, "pg_data/PG_VERSION={\"checksum\":\"%s\",\"size\":3}\n", sha1);
	fclose(fp);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_check_manifest_checksums(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	ck_assert_int_eq(r->status, BACKUP_STATUS_OK);

	free_validation_result(r);
	free(bi);
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* Checksum mismatch → 1 error */
START_TEST(test_pbr_checksums_mismatch)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pbr_ck_mis_%d", getpid());
	mkdir(dir, 0755);

	snprintf(path, sizeof(path), "%s/pg_data", dir);
	mkdir(path, 0755);

	snprintf(path, sizeof(path), "%s/pg_data/PG_VERSION", dir);
	FILE *fp = fopen(path, "w");
	fputs("17\n", fp);
	fclose(fp);

	/* Write manifest with WRONG checksum */
	snprintf(path, sizeof(path), "%s/backup.manifest", dir);
	fp = fopen(path, "w");
	fprintf(fp, "[backup]\nbackup-label=test\n\n");
	fprintf(fp, "[target:file]\n");
	fprintf(fp, "pg_data/PG_VERSION={\"checksum\":\"0000000000000000000000000000000000000000\",\"size\":3}\n");
	fclose(fp);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_check_manifest_checksums(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_ge(r->error_count, 1);
	bool found = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "SHA1 mismatch")) found = true;
	ck_assert_msg(found, "expected SHA1 mismatch error");
	ck_assert_int_eq(r->status, BACKUP_STATUS_ERROR);

	free_validation_result(r);
	free(bi);
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* Missing file listed in manifest → error */
START_TEST(test_pbr_checksums_missing_file)
{
	char dir[PATH_MAX], path[PATH_MAX];
	snprintf(dir, sizeof(dir), "/tmp/pbr_ck_mf_%d", getpid());
	mkdir(dir, 0755);

	snprintf(path, sizeof(path), "%s/pg_data", dir);
	mkdir(path, 0755);

	/* manifest lists a file that doesn't exist on disk */
	snprintf(path, sizeof(path), "%s/backup.manifest", dir);
	FILE *fp = fopen(path, "w");
	fprintf(fp, "[backup]\nbackup-label=test\n\n");
	fprintf(fp, "[target:file]\n");
	fprintf(fp, "pg_data/missing_file={\"checksum\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"size\":100}\n");
	fclose(fp);

	BackupInfo *bi = make_pgbackrest_backup_info(dir);
	ValidationResult *r = pgbackrest_check_manifest_checksums(bi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_ge(r->error_count, 1);
	bool found = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "Missing file")) found = true;
	ck_assert_msg(found, "expected 'Missing file' error");

	free_validation_result(r);
	free(bi);
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	system(cmd);
}
END_TEST

/* ------------------------------------------------------------------ *
 * pgbackrest chain validation tests
 * ------------------------------------------------------------------ */

/* Helper: make a minimal BackupInfo on heap */
static BackupInfo *
make_pbr_backup(const char *id, BackupType type,
				const char *parent_id, const char *path)
{
	BackupInfo *bi = calloc(1, sizeof(BackupInfo));
	bi->tool   = BACKUP_TOOL_PGBACKREST;
	bi->status = BACKUP_STATUS_OK;
	bi->type   = type;
	strncpy(bi->backup_id, id, sizeof(bi->backup_id) - 1);
	if (parent_id != NULL)
		strncpy(bi->parent_backup_id, parent_id,
				sizeof(bi->parent_backup_id) - 1);
	if (path != NULL)
		strncpy(bi->backup_path, path, sizeof(bi->backup_path) - 1);
	return bi;
}

/* FULL with no parent → 0 chain errors */
START_TEST(test_chain_full_no_parent)
{
	BackupInfo *full = make_pbr_backup("20240101-120000F",
									   BACKUP_TYPE_FULL, NULL, "/nonexistent");
	ValidationResult *r = validate_backup_chain(full, full, NULL,
												VALIDATION_LEVEL_STANDARD);
	ck_assert_ptr_nonnull(r);
	/* validate_backup_metadata will warn about path/lsn — we only care that
	 * no chain-specific error mentions "unexpected parent_backup_id" */
	bool chain_err = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "unexpected parent_backup_id")) chain_err = true;
	ck_assert_msg(!chain_err,
				  "FULL with no parent should not produce chain error");
	free_validation_result(r);
	free(full);
}
END_TEST

/* FULL with unexpected parent → chain error */
START_TEST(test_chain_full_with_parent)
{
	BackupInfo *full = make_pbr_backup("20240101-120000F",
									   BACKUP_TYPE_FULL,
									   "20231231-120000F", "/nonexistent");
	ValidationResult *r = validate_backup_chain(full, full, NULL,
												VALIDATION_LEVEL_STANDARD);
	ck_assert_ptr_nonnull(r);
	bool found = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "unexpected parent_backup_id")) found = true;
	ck_assert_msg(found, "expected 'unexpected parent_backup_id' error");
	free_validation_result(r);
	free(full);
}
END_TEST

/* DIFF with valid parent (FULL) → no chain error */
START_TEST(test_chain_diff_valid_parent)
{
	BackupInfo *full = make_pbr_backup("20240101-120000F",
									   BACKUP_TYPE_FULL, NULL, "/nonexistent");
	BackupInfo *diff = make_pbr_backup("20240102-120000D",
									   BACKUP_TYPE_DELTA,
									   "20240101-120000F", "/nonexistent");
	full->next = diff;

	ValidationResult *r = validate_backup_chain(diff, full, NULL,
												VALIDATION_LEVEL_STANDARD);
	ck_assert_ptr_nonnull(r);
	bool chain_err = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "not found in catalog") ||
			strstr(r->errors[i], "Broken chain"))
			chain_err = true;
	ck_assert_msg(!chain_err, "DIFF with valid parent should not have chain error");
	free_validation_result(r);
	free(full);
	free(diff);
}
END_TEST

/* INCR with missing parent → chain error */
START_TEST(test_chain_incr_missing_parent)
{
	BackupInfo *incr = make_pbr_backup("20240103-120000I",
									   BACKUP_TYPE_INCREMENTAL,
									   "20240102-120000D", "/nonexistent");
	/* catalog has only incr, no parent */
	ValidationResult *r = validate_backup_chain(incr, incr, NULL,
												VALIDATION_LEVEL_STANDARD);
	ck_assert_ptr_nonnull(r);
	bool found = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "not found in catalog") ||
			strstr(r->errors[i], "Broken chain"))
			found = true;
	ck_assert_msg(found, "expected chain error for missing parent");
	free_validation_result(r);
	free(incr);
}
END_TEST

/* INCR with no parent_backup_id at all → chain error */
START_TEST(test_chain_incr_no_parent_id)
{
	BackupInfo *incr = make_pbr_backup("20240103-120000I",
									   BACKUP_TYPE_INCREMENTAL,
									   NULL, "/nonexistent");
	ValidationResult *r = validate_backup_chain(incr, incr, NULL,
												VALIDATION_LEVEL_STANDARD);
	ck_assert_ptr_nonnull(r);
	bool found = false;
	for (int i = 0; i < r->error_count; i++)
		if (strstr(r->errors[i], "no parent_backup_id")) found = true;
	ck_assert_msg(found, "expected 'no parent_backup_id' error");
	free_validation_result(r);
	free(incr);
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
	TCase *tc_validate;

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
	tcase_add_test(tc_metadata, test_parse_tool_version);
	tcase_add_test(tc_metadata, test_backup_type_parsing);
	suite_add_tcase(s, tc_metadata);

	/* Test case for structure validation */
	tc_validate = tcase_create("validate_structure");
	tcase_add_test(tc_validate, test_validate_struct_null_input);
	tcase_add_test(tc_validate, test_validate_struct_ok);
	tcase_add_test(tc_validate, test_validate_struct_ok_without_copy);
	tcase_add_test(tc_validate, test_validate_struct_missing_pg_control);
	tcase_add_test(tc_validate, test_validate_struct_missing_pg_version);
	tcase_add_test(tc_validate, test_validate_struct_missing_manifest);
	tcase_add_test(tc_validate, test_validate_struct_missing_pg_data);
	tcase_add_test(tc_validate, test_validate_struct_backup_error);
	suite_add_tcase(s, tc_validate);

	/* Test case for manifest checksums */
	TCase *tc_checksums = tcase_create("manifest_checksums");
	tcase_add_test(tc_checksums, test_pbr_checksums_null_input);
	tcase_add_test(tc_checksums, test_pbr_checksums_no_manifest);
	tcase_add_test(tc_checksums, test_pbr_checksums_compressed);
	tcase_add_test(tc_checksums, test_pbr_checksums_all_ok);
	tcase_add_test(tc_checksums, test_pbr_checksums_mismatch);
	tcase_add_test(tc_checksums, test_pbr_checksums_missing_file);
	suite_add_tcase(s, tc_checksums);

	/* Test case for chain validation */
	TCase *tc_chain = tcase_create("chain_validation");
	tcase_add_test(tc_chain, test_chain_full_no_parent);
	tcase_add_test(tc_chain, test_chain_full_with_parent);
	tcase_add_test(tc_chain, test_chain_diff_valid_parent);
	tcase_add_test(tc_chain, test_chain_incr_missing_parent);
	tcase_add_test(tc_chain, test_chain_incr_no_parent_id);
	suite_add_tcase(s, tc_chain);

	return s;
}

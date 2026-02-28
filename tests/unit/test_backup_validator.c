/*
 * test_backup_validator.c
 *
 * Unit tests for backup_validator: check_backup_checksums, compute_file_crc32c
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


#define _POSIX_C_SOURCE 200809L
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stdint.h>
#include <unistd.h>
#include <check.h>
#include "../../include/types.h"
#include "../../include/common.h"

/* ------------------------------------------------------------------ *
 * Helpers
 * ------------------------------------------------------------------ */

static char test_dir[PATH_MAX];
static char backup_dir[PATH_MAX];

/*
 * Create a directory tree: backup_dir/database/subdir
 */
static void
make_dir(const char *path)
{
	char tmp[PATH_MAX];
	char *p;

	strncpy(tmp, path, sizeof(tmp) - 1);
	tmp[sizeof(tmp) - 1] = '\0';

	for (p = tmp + 1; *p; p++)
	{
		if (*p == '/')
		{
			*p = '\0';
			mkdir(tmp, 0755);
			*p = '/';
		}
	}
	mkdir(tmp, 0755);
}

/*
 * Write bytes to a file, creating parent directories as needed.
 */
static void
write_file(const char *path, const void *data, size_t len)
{
	FILE *fp = fopen(path, "wb");
	if (fp)
	{
		fwrite(data, 1, len, fp);
		fclose(fp);
	}
}

/*
 * Compute CRC32C of a byte buffer (same algorithm as production code).
 */
static uint32_t
buf_crc32c(const uint8_t *buf, size_t len)
{
	static uint32_t table[256];
	static int      initialized = 0;
	const uint32_t  poly = 0x82F63B78U;

	if (!initialized)
	{
		for (int i = 0; i < 256; i++)
		{
			uint32_t crc = (uint32_t) i;
			for (int j = 0; j < 8; j++)
				crc = (crc & 1) ? ((crc >> 1) ^ poly) : (crc >> 1);
			table[i] = crc;
		}
		initialized = 1;
	}

	uint32_t crc = ~0U;
	for (size_t i = 0; i < len; i++)
		crc = (crc >> 8) ^ table[(crc ^ buf[i]) & 0xFFU];
	return ~crc;
}

/*
 * Write a single-entry backup_content.control JSON line.
 *
 * path      - relative path inside database/
 * kind      - "reg" or "dir"
 * size      - file size (bytes)
 * crc       - CRC32C value to write
 * compress  - "none" or e.g. "zlib"
 */
static void
write_content_control(const char *bdir, const char *path,
					  const char *kind, size_t size,
					  uint32_t crc, const char *compress)
{
	char ctrl_path[PATH_MAX];
	FILE *fp;

	snprintf(ctrl_path, sizeof(ctrl_path), "%s/backup_content.control", bdir);
	fp = fopen(ctrl_path, "w");
	if (!fp) return;

	fprintf(fp,
			"{\"path\":\"%s\", \"size\":\"%zu\", \"kind\":\"%s\","
			" \"mode\":\"384\", \"is_datafile\":\"0\", \"is_cfs\":\"0\","
			" \"is_tde\":\"0\", \"crc\":\"%u\", \"compress_alg\":\"%s\","
			" \"external_dir_num\":\"0\"}\n",
			path, size, kind, crc, compress);
	fclose(fp);
}

static void
setup_test_dirs(void)
{
	snprintf(test_dir, sizeof(test_dir), "/tmp/pg_bv_test_%d", getpid());
	snprintf(backup_dir, sizeof(backup_dir), "%s/backup", test_dir);

	make_dir(test_dir);
	make_dir(backup_dir);
}

static void
teardown_test_dirs(void)
{
	char cmd[PATH_MAX + 16];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
	system(cmd);
}

/*
 * Populate a BackupInfo with minimal fields for testing.
 */
static BackupInfo
make_backup_info(BackupType type)
{
	BackupInfo info;
	memset(&info, 0, sizeof(info));
	strncpy(info.backup_id, "TESTID", sizeof(info.backup_id) - 1);
	strncpy(info.backup_path, backup_dir, sizeof(info.backup_path) - 1);
	info.type   = type;
	info.tool   = BACKUP_TOOL_PG_PROBACKUP;
	info.status = BACKUP_STATUS_OK;
	return info;
}

/* ------------------------------------------------------------------ *
 * compute_file_crc32c tests
 * ------------------------------------------------------------------ */

/* CRC32C of "123456789" is the well-known test vector 0xE3069283 */
START_TEST(test_crc_known_vector)
{
	const char *data = "123456789";
	char        path[PATH_MAX];

	setup_test_dirs();
	snprintf(path, sizeof(path), "%s/crc_test.bin", test_dir);
	write_file(path, data, strlen(data));

	uint32_t crc = 0;
	bool     ok  = compute_file_crc32c(path, &crc);

	ck_assert_msg(ok, "compute_file_crc32c should succeed");
	ck_assert_uint_eq(crc, 0xE3069283U);

	teardown_test_dirs();
}
END_TEST

START_TEST(test_crc_nonexistent_file)
{
	uint32_t crc = 0;
	bool     ok  = compute_file_crc32c("/tmp/this_file_does_not_exist_xyz", &crc);
	ck_assert_msg(!ok, "compute_file_crc32c should fail for missing file");
}
END_TEST

/* ------------------------------------------------------------------ *
 * check_backup_checksums tests
 * ------------------------------------------------------------------ */

/* No backup_content.control → returns NULL (tool not supported) */
START_TEST(test_no_content_control)
{
	setup_test_dirs();

	BackupInfo     info   = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_null(res);

	teardown_test_dirs();
}
END_TEST

/* FULL backup, single file with correct CRC → 0 errors */
START_TEST(test_full_backup_file_ok)
{
	const uint8_t data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
	uint32_t      crc    = buf_crc32c(data, sizeof(data));
	char          file_path[PATH_MAX];

	setup_test_dirs();

	/* Create database/base/1/ and write the file */
	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);
	snprintf(file_path, sizeof(file_path), "%s/1234", db_base);
	write_file(file_path, data, sizeof(data));

	write_content_control(backup_dir, "base/1/1234", "reg",
						  sizeof(data), crc, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* FULL backup, non-zero size file missing → 1 error */
START_TEST(test_full_backup_missing_file)
{
	setup_test_dirs();

	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);
	/* Do NOT create the data file */

	write_content_control(backup_dir, "base/1/9999", "reg",
						  1024, 0xDEADBEEFU, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 1);
	ck_assert_ptr_nonnull(strstr(res->errors[0], "Missing file"));
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* DELTA backup, file missing from local database/ → 0 errors (belongs to parent) */
START_TEST(test_delta_backup_missing_file_ok)
{
	setup_test_dirs();

	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);
	/* Do NOT create the data file */

	write_content_control(backup_dir, "base/1/9999", "reg",
						  1024, 0xDEADBEEFU, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_DELTA);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* CRC32C mismatch → 1 error */
START_TEST(test_crc_mismatch)
{
	const uint8_t data[] = {0xAA, 0xBB, 0xCC};
	char          file_path[PATH_MAX];

	setup_test_dirs();

	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);
	snprintf(file_path, sizeof(file_path), "%s/5678", db_base);
	write_file(file_path, data, sizeof(data));

	/* Write WRONG crc */
	write_content_control(backup_dir, "base/1/5678", "reg",
						  sizeof(data), 0xDEADBEEFU, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 1);
	ck_assert_ptr_nonnull(strstr(res->errors[0], "CRC32C mismatch"));
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* Size mismatch → 1 error */
START_TEST(test_size_mismatch)
{
	const uint8_t data[] = {0x01, 0x02, 0x03};
	uint32_t      crc    = buf_crc32c(data, sizeof(data));
	char          file_path[PATH_MAX];

	setup_test_dirs();

	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);
	snprintf(file_path, sizeof(file_path), "%s/3333", db_base);
	write_file(file_path, data, sizeof(data));

	/* Write WRONG size (actual is 3, stored claims 999) */
	write_content_control(backup_dir, "base/1/3333", "reg",
						  999, crc, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 1);
	ck_assert_ptr_nonnull(strstr(res->errors[0], "Size mismatch"));
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* Zero-size entry: file not on disk → 0 errors (skipped) */
START_TEST(test_zero_size_file_skipped)
{
	setup_test_dirs();

	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);
	/* Do NOT create the data file */

	write_content_control(backup_dir, "base/1/7777", "reg",
						  0, 0, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* global/pg_control with wrong CRC → 0 errors (excluded from CRC check) */
START_TEST(test_pg_control_excluded)
{
	const uint8_t data[16384] = {0};  /* 16 KB of zeros */
	char          file_path[PATH_MAX];

	setup_test_dirs();

	char global_dir[PATH_MAX];
	snprintf(global_dir, sizeof(global_dir), "%s/database/global", backup_dir);
	make_dir(global_dir);
	snprintf(file_path, sizeof(file_path), "%s/pg_control", global_dir);
	write_file(file_path, data, sizeof(data));

	/* Write deliberately wrong CRC */
	write_content_control(backup_dir, "global/pg_control", "reg",
						  sizeof(data), 0xDEADBEEFU, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* Directory entries are silently skipped */
START_TEST(test_dir_entries_skipped)
{
	setup_test_dirs();

	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);

	/* Write a "dir" kind entry — should be ignored */
	write_content_control(backup_dir, "base/1", "dir",
						  0, 0, "none");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* Compressed file: CRC check skipped, only existence + size checked */
START_TEST(test_compressed_file_skips_crc)
{
	const uint8_t data[] = {0x1F, 0x8B, 0x08};  /* gzip magic bytes */
	char          file_path[PATH_MAX];

	setup_test_dirs();

	char db_base[PATH_MAX];
	snprintf(db_base, sizeof(db_base), "%s/database/base/1", backup_dir);
	make_dir(db_base);
	snprintf(file_path, sizeof(file_path), "%s/2222", db_base);
	write_file(file_path, data, sizeof(data));

	/* compress_alg != "none", so CRC check is skipped */
	write_content_control(backup_dir, "base/1/2222", "reg",
						  sizeof(data), 0xDEADBEEFU, "zlib");

	BackupInfo       info = make_backup_info(BACKUP_TYPE_FULL);
	ValidationResult *res = check_backup_checksums(&info);

	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	free_validation_result(res);

	teardown_test_dirs();
}
END_TEST

/* ================================================================== *
 * validate_backup_metadata() tests
 * ================================================================== */

/*
 * Helper: build a fully valid BackupInfo pointing to an existing directory.
 */
static void
fill_valid_backup(BackupInfo *b)
{
	memset(b, 0, sizeof(*b));
	strcpy(b->backup_id, "TEST0001");
	strcpy(b->backup_path, "/tmp");   /* always exists */
	b->start_time = 1000;
	b->end_time   = 2000;
	b->start_lsn  = 0x1000000;
	b->stop_lsn   = 0x2000000;
	b->timeline   = 1;
	b->pg_version = 170000;
	b->status     = BACKUP_STATUS_OK;
}

/* NULL input → NULL returned (no crash) */
START_TEST(test_meta_null_info)
{
	ValidationResult *res = validate_backup_metadata(NULL);
	ck_assert_ptr_null(res);
}
END_TEST

/* All fields valid → OK, 0 errors, 0 warnings */
START_TEST(test_meta_valid_backup)
{
	BackupInfo b;
	fill_valid_backup(&b);

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count,   0);
	ck_assert_int_eq(res->warning_count, 0);
	ck_assert_int_eq(res->status, BACKUP_STATUS_OK);
	free_validation_result(res);
}
END_TEST

/* Empty backup_id → 1 error */
START_TEST(test_meta_missing_backup_id)
{
	BackupInfo b;
	fill_valid_backup(&b);
	b.backup_id[0] = '\0';

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_gt(res->error_count, 0);
	ck_assert(strstr(res->errors[0], "backup_id") != NULL);
	free_validation_result(res);
}
END_TEST

/* Empty backup_path → 1 error */
START_TEST(test_meta_missing_backup_path)
{
	BackupInfo b;
	fill_valid_backup(&b);
	b.backup_path[0] = '\0';

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_gt(res->error_count, 0);
	ck_assert(strstr(res->errors[0], "backup_path") != NULL);
	free_validation_result(res);
}
END_TEST

/* Non-existent backup_path → 1 error "does not exist" */
START_TEST(test_meta_nonexistent_path)
{
	BackupInfo b;
	fill_valid_backup(&b);
	strcpy(b.backup_path, "/tmp/pg_bv_nonexistent_path_12345");

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_gt(res->error_count, 0);
	ck_assert(strstr(res->errors[0], "does not exist") != NULL);
	free_validation_result(res);
}
END_TEST

/* start_time == 0 → 1 warning "Missing start_time" */
START_TEST(test_meta_missing_start_time)
{
	BackupInfo b;
	fill_valid_backup(&b);
	b.start_time = 0;

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	ck_assert_int_gt(res->warning_count, 0);
	ck_assert(strstr(res->warnings[0], "start_time") != NULL);
	free_validation_result(res);
}
END_TEST

/* status=OK but end_time==0 → warning "Missing end_time" */
START_TEST(test_meta_missing_end_time)
{
	BackupInfo b;
	fill_valid_backup(&b);
	b.end_time = 0;
	b.status   = BACKUP_STATUS_OK;

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_eq(res->error_count, 0);
	ck_assert_int_gt(res->warning_count, 0);
	bool found = false;
	for (int i = 0; i < res->warning_count; i++)
		if (strstr(res->warnings[i], "end_time")) found = true;
	ck_assert(found);
	free_validation_result(res);
}
END_TEST

/* start_time > end_time → 1 error "Invalid timestamps" */
START_TEST(test_meta_invalid_timestamps)
{
	BackupInfo b;
	fill_valid_backup(&b);
	b.start_time = 9000;
	b.end_time   = 1000;

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_gt(res->error_count, 0);
	ck_assert(strstr(res->errors[0], "Invalid timestamps") != NULL);
	free_validation_result(res);
}
END_TEST

/* start_lsn >= stop_lsn → 1 error "Invalid LSN range" */
START_TEST(test_meta_invalid_lsn)
{
	BackupInfo b;
	fill_valid_backup(&b);
	b.start_lsn = 0x3000000;
	b.stop_lsn  = 0x1000000;

	ValidationResult *res = validate_backup_metadata(&b);
	ck_assert_ptr_nonnull(res);
	ck_assert_int_gt(res->error_count, 0);
	ck_assert(strstr(res->errors[0], "LSN") != NULL);
	free_validation_result(res);
}
END_TEST

/* ------------------------------------------------------------------ *
 * Suite assembly
 * ------------------------------------------------------------------ */

Suite*
backup_validator_suite(void)
{
	Suite *s = suite_create("backup_validator");

	TCase *tc_crc = tcase_create("compute_file_crc32c");
	tcase_add_test(tc_crc, test_crc_known_vector);
	tcase_add_test(tc_crc, test_crc_nonexistent_file);
	suite_add_tcase(s, tc_crc);

	TCase *tc_chk = tcase_create("check_backup_checksums");
	tcase_add_test(tc_chk, test_no_content_control);
	tcase_add_test(tc_chk, test_full_backup_file_ok);
	tcase_add_test(tc_chk, test_full_backup_missing_file);
	tcase_add_test(tc_chk, test_delta_backup_missing_file_ok);
	tcase_add_test(tc_chk, test_crc_mismatch);
	tcase_add_test(tc_chk, test_size_mismatch);
	tcase_add_test(tc_chk, test_zero_size_file_skipped);
	tcase_add_test(tc_chk, test_pg_control_excluded);
	tcase_add_test(tc_chk, test_dir_entries_skipped);
	tcase_add_test(tc_chk, test_compressed_file_skips_crc);
	suite_add_tcase(s, tc_chk);

	TCase *tc_meta = tcase_create("validate_backup_metadata");
	tcase_add_test(tc_meta, test_meta_null_info);
	tcase_add_test(tc_meta, test_meta_valid_backup);
	tcase_add_test(tc_meta, test_meta_missing_backup_id);
	tcase_add_test(tc_meta, test_meta_missing_backup_path);
	tcase_add_test(tc_meta, test_meta_nonexistent_path);
	tcase_add_test(tc_meta, test_meta_missing_start_time);
	tcase_add_test(tc_meta, test_meta_missing_end_time);
	tcase_add_test(tc_meta, test_meta_invalid_timestamps);
	tcase_add_test(tc_meta, test_meta_invalid_lsn);
	suite_add_tcase(s, tc_meta);

	return s;
}

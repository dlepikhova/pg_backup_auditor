/*
 * test_wal_validator.c
 *
 * Unit tests for WAL validation
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
#include <check.h>
#include "../../include/types.h"
#include "../../include/common.h"
#include "../../include/adapter.h"

/*
 * Helper: Create a WALArchiveInfo with test segments
 */
static WALArchiveInfo*
create_test_wal_archive(int count)
{
	WALArchiveInfo *info;
	int i;

	info = calloc(1, sizeof(WALArchiveInfo));
	if (info == NULL)
		return NULL;

	strcpy(info->archive_path, "/test/wal/archive");
	info->segment_count = count;

	if (count > 0)
	{
		info->segments = malloc(count * sizeof(WALSegmentName));
		if (info->segments == NULL)
		{
			free(info);
			return NULL;
		}

		/* Create sequential segments: timeline=1, log_id=0, seg_id=0..count-1 */
		for (i = 0; i < count; i++)
		{
			info->segments[i].timeline = 1;
			info->segments[i].log_id = 0;
			info->segments[i].seg_id = i;
		}
	}

	return info;
}

/*
 * Test: Check WAL availability - all segments present
 */
START_TEST(test_check_wal_availability_all_present)
{
	BackupInfo backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	/* Setup backup that needs segments 0-4
	 * start_lsn = 0x0 → segment 0
	 * stop_lsn = 0x4000000 → segment 4 */
	memset(&backup, 0, sizeof(backup));
	strcpy(backup.backup_id, "test-backup");
	backup.timeline = 1;
	backup.start_lsn = 0x0;
	backup.stop_lsn = 0x4000000;  /* 4 * 16MB */

	/* Create WAL archive with segments 0-9 */
	wal_info = create_test_wal_archive(10);

	/* Check availability */
	result = check_wal_availability(&backup, wal_info);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->status, BACKUP_STATUS_OK);
	ck_assert_int_eq(result->error_count, 0);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
}
END_TEST

/*
 * Test: Check WAL availability - missing segments
 */
START_TEST(test_check_wal_availability_missing)
{
	BackupInfo backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	/* Setup backup that needs segments 0-9
	 * start_lsn = 0x0 → segment 0
	 * stop_lsn = 0x9000000 → segment 9 */
	memset(&backup, 0, sizeof(backup));
	strcpy(backup.backup_id, "test-backup");
	backup.timeline = 1;
	backup.start_lsn = 0x0;
	backup.stop_lsn = 0x9000000;  /* 9 * 16MB */

	/* Create WAL archive with only segments 0-4 (missing 5-9) */
	wal_info = create_test_wal_archive(5);

	/* Check availability */
	result = check_wal_availability(&backup, wal_info);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->status, BACKUP_STATUS_ERROR);
	ck_assert(result->error_count > 0);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
}
END_TEST

/*
 * Test: Check WAL availability - backup without LSN info
 */
START_TEST(test_check_wal_availability_no_lsn)
{
	BackupInfo backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	/* Setup backup without LSN information */
	memset(&backup, 0, sizeof(backup));
	strcpy(backup.backup_id, "test-backup");
	backup.timeline = 1;
	backup.start_lsn = 0;
	backup.stop_lsn = 0;

	/* Create WAL archive */
	wal_info = create_test_wal_archive(5);

	/* Check availability */
	result = check_wal_availability(&backup, wal_info);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->status, BACKUP_STATUS_WARNING);
	ck_assert_int_eq(result->warning_count, 1);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
}
END_TEST

/*
 * Test: Check WAL availability - single segment
 */
START_TEST(test_check_wal_availability_single_segment)
{
	BackupInfo backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	/* Setup backup that needs only segment 0
	 * start_lsn = 0x100 → segment 0
	 * stop_lsn = 0x200 → segment 0 */
	memset(&backup, 0, sizeof(backup));
	strcpy(backup.backup_id, "test-backup");
	backup.timeline = 1;
	backup.start_lsn = 0x100;
	backup.stop_lsn = 0x200;

	/* Create WAL archive with segment 0 */
	wal_info = create_test_wal_archive(1);

	/* Check availability */
	result = check_wal_availability(&backup, wal_info);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->status, BACKUP_STATUS_OK);
	ck_assert_int_eq(result->error_count, 0);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
}
END_TEST

/*
 * Test: Check WAL availability - gap in middle
 */
START_TEST(test_check_wal_availability_gap)
{
	BackupInfo backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	/* Setup backup that needs segments 0-4 */
	memset(&backup, 0, sizeof(backup));
	strcpy(backup.backup_id, "test-backup");
	backup.timeline = 1;
	backup.start_lsn = 0x0;
	backup.stop_lsn = 0x4000000;  /* 4 * 16MB */

	/* Create WAL archive with segments 0,1,3,4 (missing 2) */
	wal_info = create_test_wal_archive(5);
	/* Remove segment 2 by shifting segments 3,4 down */
	wal_info->segments[2] = wal_info->segments[3];
	wal_info->segments[3] = wal_info->segments[4];
	wal_info->segment_count = 4;

	/* Check availability */
	result = check_wal_availability(&backup, wal_info);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->status, BACKUP_STATUS_ERROR);
	ck_assert(result->error_count > 0);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
}
END_TEST

/*
 * Test: Check WAL availability - empty archive
 */
START_TEST(test_check_wal_availability_empty_archive)
{
	BackupInfo backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	/* Setup backup that needs segments */
	memset(&backup, 0, sizeof(backup));
	strcpy(backup.backup_id, "test-backup");
	backup.timeline = 1;
	backup.start_lsn = 0x0;
	backup.stop_lsn = 0x1000000;

	/* Create empty WAL archive */
	wal_info = create_test_wal_archive(0);

	/* Check availability */
	result = check_wal_availability(&backup, wal_info);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->status, BACKUP_STATUS_ERROR);
	ck_assert(result->error_count > 0);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
}
END_TEST

/* -----------------------------------------------------------------------
 * Integration tests: real pg_probackup backup
 * These tests are skipped if the backup catalog is not present.
 * ----------------------------------------------------------------------- */

#define REAL_CATALOG    "/Users/daria/projects/postgrespro/backup"
#define REAL_INSTANCE   "test"
#define REAL_BACKUP_ID  "TB50D5"

/*
 * get_wal_archive_path() returns the correct default path for pg_probackup
 */
START_TEST(test_pg_probackup_wal_path_detection)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s", REAL_CATALOG, REAL_INSTANCE, REAL_BACKUP_ID);

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path, REAL_INSTANCE);

	ck_assert_ptr_nonnull(wal_path);

	/* Expected: <catalog>/wal/<instance> */
	char expected[PATH_MAX];
	snprintf(expected, sizeof(expected),
			 "%s/wal/%s", REAL_CATALOG, REAL_INSTANCE);
	ck_assert_str_eq(wal_path, expected);

	free(wal_path);
}
END_TEST

/*
 * Full end-to-end: scan real backup, find WAL, check availability
 */
START_TEST(test_pg_probackup_wal_availability_real)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s", REAL_CATALOG, REAL_INSTANCE, REAL_BACKUP_ID);

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	/* Scan backup metadata */
	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);
	ck_assert_str_eq(backup->backup_id, REAL_BACKUP_ID);

	/* LSN must be parsed from backup.control */
	ck_assert(backup->start_lsn != 0 || backup->stop_lsn != 0);

	/* Find WAL archive */
	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path, backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	/* Scan WAL archive */
	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);
	ck_assert_int_gt(wal_info->segment_count, 0);

	/* Check WAL availability for the backup */
	result = check_wal_availability(backup, wal_info);
	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->status, BACKUP_STATUS_OK);
	ck_assert_int_eq(result->error_count, 0);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
	free(wal_path);
	free_backup_list(backup);
}
END_TEST

/*
 * Negative scenario: rename one required WAL segment and check that
 * check_wal_availability() reports it as missing.
 * The segment is restored after the test.
 */
START_TEST(test_pg_probackup_wal_missing_segment)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	char seg_original[PATH_MAX];
	char seg_renamed[PATH_MAX];
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;
	int rc;

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s", REAL_CATALOG, REAL_INSTANCE, REAL_BACKUP_ID);

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path, backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	/* Rename segment 000000010000000000000002 — it is required by this backup */
	snprintf(seg_original, sizeof(seg_original),
			 "%s/000000010000000000000002", wal_path);
	snprintf(seg_renamed, sizeof(seg_renamed),
			 "%s/000000010000000000000002.broken", wal_path);

	rc = rename(seg_original, seg_renamed);
	ck_assert_int_eq(rc, 0);

	/* Rescan archive — segment must be missing now */
	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);

	result = check_wal_availability(backup, wal_info);
	ck_assert_ptr_nonnull(result);

	/* Must report error and name the missing segment */
	ck_assert_int_eq(result->status, BACKUP_STATUS_ERROR);
	ck_assert_int_gt(result->error_count, 0);
	ck_assert_ptr_nonnull(result->errors[0]);
	ck_assert(strstr(result->errors[0], "000000010000000000000002") != NULL);

	free_validation_result(result);
	free_wal_archive_info(wal_info);

	/* Restore segment */
	rc = rename(seg_renamed, seg_original);
	ck_assert_int_eq(rc, 0);

	free(wal_path);
	free_backup_list(backup);
}
END_TEST

/* -----------------------------------------------------------------------
 * Helpers for header-level negative tests
 * ----------------------------------------------------------------------- */

#define WAL_HDR_LEN 40  /* XLogLongPageHeaderData size (with alignment padding) */

/*
 * Read the first WAL_HDR_LEN bytes from a segment file.
 * Returns true on success.
 */
static bool
read_seg_header(const char *path, uint8_t buf[WAL_HDR_LEN])
{
	FILE *f = fopen(path, "rb");
	if (!f) return false;
	bool ok = (fread(buf, 1, WAL_HDR_LEN, f) == WAL_HDR_LEN);
	fclose(f);
	return ok;
}

/*
 * Overwrite the first WAL_HDR_LEN bytes of a segment file.
 * Returns true on success.
 */
static bool
write_seg_header(const char *path, const uint8_t buf[WAL_HDR_LEN])
{
	FILE *f = fopen(path, "r+b");
	if (!f) return false;
	bool ok = (fwrite(buf, 1, WAL_HDR_LEN, f) == WAL_HDR_LEN);
	fclose(f);
	return ok;
}

/* -----------------------------------------------------------------------
 * Negative tests for check_wal_headers()
 * All tests skip if the real backup is not present.
 * All tests restore the modified files before asserting results,
 * so failures leave the archive in a clean state.
 * ----------------------------------------------------------------------- */

/*
 * Rename the required segment 000000010000000000000002.
 * check_wal_availability should report it missing.
 * check_wal_headers should return OK for the remaining segment (seg 003).
 */
START_TEST(test_pg_probackup_wal_headers_missing_seg)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	char seg_orig[PATH_MAX], seg_broken[PATH_MAX];
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;
	int rc;

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s", REAL_CATALOG, REAL_INSTANCE, REAL_BACKUP_ID);

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path, backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	snprintf(seg_orig,   sizeof(seg_orig),   "%s/000000010000000000000002", wal_path);
	snprintf(seg_broken, sizeof(seg_broken),  "%s/000000010000000000000002.broken", wal_path);

	rc = rename(seg_orig, seg_broken);
	ck_assert_int_eq(rc, 0);

	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);

	result = check_wal_headers(backup, wal_info);

	/* Restore before assertions so archive is never left dirty */
	rename(seg_broken, seg_orig);

	ck_assert_ptr_nonnull(result);
	/* Header check skips missing segments — no header errors expected */
	ck_assert_int_eq(result->error_count, 0);
	ck_assert_int_eq(result->status, BACKUP_STATUS_OK);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
	free(wal_path);
	free_backup_list(backup);
}
END_TEST

/*
 * Patch xlp_tli in segment 000000010000000000000003 to timeline 99.
 * check_wal_headers should report a timeline mismatch for that segment.
 */
START_TEST(test_pg_probackup_wal_headers_bad_tli)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	char seg003[PATH_MAX];
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;
	uint8_t orig[WAL_HDR_LEN], patched[WAL_HDR_LEN];

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s", REAL_CATALOG, REAL_INSTANCE, REAL_BACKUP_ID);

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path, backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	snprintf(seg003, sizeof(seg003), "%s/000000010000000000000003", wal_path);

	ck_assert(read_seg_header(seg003, orig));

	/* Patch: set xlp_tli (offset 4, uint32 LE) to 99 */
	memcpy(patched, orig, WAL_HDR_LEN);
	patched[4] = 99; patched[5] = 0; patched[6] = 0; patched[7] = 0;
	ck_assert(write_seg_header(seg003, patched));

	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);

	result = check_wal_headers(backup, wal_info);

	/* Restore before asserting */
	write_seg_header(seg003, orig);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_gt(result->error_count, 0);
	ck_assert_int_eq(result->status, BACKUP_STATUS_ERROR);
	ck_assert_ptr_nonnull(result->errors[0]);
	ck_assert(strstr(result->errors[0], "timeline mismatch") != NULL);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
	free(wal_path);
	free_backup_list(backup);
}
END_TEST

/*
 * Copy the header of segment 000000010000000000000002 into segment 000000010000000000000003.
 * The header will say xlp_pageaddr=0x2000000 but we expect 0x3000000 for segment 3.
 * check_wal_headers should report a page address mismatch.
 */
START_TEST(test_pg_probackup_wal_headers_bad_pageaddr)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	char seg002[PATH_MAX], seg003[PATH_MAX];
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;
	uint8_t hdr002[WAL_HDR_LEN], orig003[WAL_HDR_LEN];

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s", REAL_CATALOG, REAL_INSTANCE, REAL_BACKUP_ID);

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path, backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	snprintf(seg002, sizeof(seg002), "%s/000000010000000000000002", wal_path);
	snprintf(seg003, sizeof(seg003), "%s/000000010000000000000003", wal_path);

	ck_assert(read_seg_header(seg002, hdr002));
	ck_assert(read_seg_header(seg003, orig003));

	/* Overwrite seg003 header with seg002 header (wrong xlp_pageaddr) */
	ck_assert(write_seg_header(seg003, hdr002));

	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);

	result = check_wal_headers(backup, wal_info);

	/* Restore before asserting */
	write_seg_header(seg003, orig003);

	ck_assert_ptr_nonnull(result);
	ck_assert_int_gt(result->error_count, 0);
	ck_assert_int_eq(result->status, BACKUP_STATUS_ERROR);
	ck_assert_ptr_nonnull(result->errors[0]);
	ck_assert(strstr(result->errors[0], "page address mismatch") != NULL);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
	free(wal_path);
	free_backup_list(backup);
}
END_TEST

/*
 * Full end-to-end: scan real backup, scan WAL, validate segment headers.
 * Expects all headers to be valid (xlp_magic != 0, XLP_LONG_HEADER set,
 * correct timeline and page address).
 */
START_TEST(test_pg_probackup_wal_headers_real)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s", REAL_CATALOG, REAL_INSTANCE, REAL_BACKUP_ID);

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path, backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);

	result = check_wal_headers(backup, wal_info);
	ck_assert_ptr_nonnull(result);
	ck_assert_int_eq(result->error_count, 0);
	ck_assert_int_eq(result->status, BACKUP_STATUS_OK);

	free_validation_result(result);
	free_wal_archive_info(wal_info);
	free(wal_path);
	free_backup_list(backup);
}
END_TEST

/*
 * Test Suite
 */
Suite*
wal_validator_suite(void)
{
	Suite *s;
	TCase *tc_availability;
	TCase *tc_integration;

	s = suite_create("WAL Validator");

	/* WAL availability unit tests */
	tc_availability = tcase_create("Availability");
	tcase_add_test(tc_availability, test_check_wal_availability_all_present);
	tcase_add_test(tc_availability, test_check_wal_availability_missing);
	tcase_add_test(tc_availability, test_check_wal_availability_no_lsn);
	tcase_add_test(tc_availability, test_check_wal_availability_single_segment);
	tcase_add_test(tc_availability, test_check_wal_availability_gap);
	tcase_add_test(tc_availability, test_check_wal_availability_empty_archive);
	suite_add_tcase(s, tc_availability);

	/* Integration tests with real pg_probackup backup */
	tc_integration = tcase_create("Integration");
	tcase_add_test(tc_integration, test_pg_probackup_wal_path_detection);
	tcase_add_test(tc_integration, test_pg_probackup_wal_availability_real);
	tcase_add_test(tc_integration, test_pg_probackup_wal_missing_segment);
	tcase_add_test(tc_integration, test_pg_probackup_wal_headers_real);
	tcase_add_test(tc_integration, test_pg_probackup_wal_headers_missing_seg);
	tcase_add_test(tc_integration, test_pg_probackup_wal_headers_bad_tli);
	tcase_add_test(tc_integration, test_pg_probackup_wal_headers_bad_pageaddr);
	suite_add_tcase(s, tc_integration);

	return s;
}

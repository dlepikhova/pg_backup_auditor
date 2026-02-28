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
#include "test_integration_helpers.h"

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
 * These tests are skipped if the required environment variables are not set
 * or if the backup catalog is not present at the given path.
 * ----------------------------------------------------------------------- */

/*
 * get_wal_archive_path() returns the correct default path for pg_probackup
 */
START_TEST(test_pg_probackup_wal_path_detection)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;

	INTEGRATION_SKIP_BACKUP();

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path,
														 get_test_instance());
	ck_assert_ptr_nonnull(wal_path);

	/* Expected: <catalog>/wal/<instance> */
	char expected[PATH_MAX];
	snprintf(expected, sizeof(expected),
			 "%s/wal/%s", get_test_catalog(), get_test_instance());
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

	INTEGRATION_SKIP_BACKUP();

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	/* Scan backup metadata */
	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);
	ck_assert_str_eq(backup->backup_id, get_test_backup_id());

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
	WALSegmentName seg_a_info;
	char seg_a_name[25];  /* "TTTTTTTTLLLLLLLLSSSSSSSS\0" */
	int rc;

	INTEGRATION_SKIP_BACKUP();

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	/* Derive the first required segment from the backup's start LSN */
	lsn_to_seg(backup->start_lsn, backup->timeline, &seg_a_info, 0);
	format_wal_filename(&seg_a_info, seg_a_name, sizeof(seg_a_name));

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path,
														 backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	snprintf(seg_original, sizeof(seg_original), "%s/%s",        wal_path, seg_a_name);
	snprintf(seg_renamed,  sizeof(seg_renamed),  "%s/%s.broken", wal_path, seg_a_name);

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
	ck_assert(strstr(result->errors[0], seg_a_name) != NULL);

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
 * Rename the first required segment (derived from start_lsn).
 * check_wal_headers should return OK for the remaining segment (stop_lsn seg).
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
	WALSegmentName seg_a_info;
	char seg_a_name[25];
	int rc;

	INTEGRATION_SKIP_BACKUP();

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	lsn_to_seg(backup->start_lsn, backup->timeline, &seg_a_info, 0);
	format_wal_filename(&seg_a_info, seg_a_name, sizeof(seg_a_name));

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path,
														 backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	snprintf(seg_orig,   sizeof(seg_orig),   "%s/%s",        wal_path, seg_a_name);
	snprintf(seg_broken, sizeof(seg_broken),  "%s/%s.broken", wal_path, seg_a_name);

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
 * Patch xlp_tli in the stop_lsn segment to timeline 99.
 * check_wal_headers should report a timeline mismatch for that segment.
 */
START_TEST(test_pg_probackup_wal_headers_bad_tli)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	char seg_b_path[PATH_MAX];
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;
	WALSegmentName seg_b_info;
	char seg_b_name[25];
	uint8_t orig[WAL_HDR_LEN], patched[WAL_HDR_LEN];

	INTEGRATION_SKIP_BACKUP();

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	lsn_to_seg(backup->stop_lsn, backup->timeline, &seg_b_info, 0);
	format_wal_filename(&seg_b_info, seg_b_name, sizeof(seg_b_name));

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path,
														 backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	snprintf(seg_b_path, sizeof(seg_b_path), "%s/%s", wal_path, seg_b_name);

	ck_assert(read_seg_header(seg_b_path, orig));

	/* Patch: set xlp_tli (offset 4, uint32 LE) to 99 */
	memcpy(patched, orig, WAL_HDR_LEN);
	patched[4] = 99; patched[5] = 0; patched[6] = 0; patched[7] = 0;
	ck_assert(write_seg_header(seg_b_path, patched));

	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);

	result = check_wal_headers(backup, wal_info);

	/* Restore before asserting */
	write_seg_header(seg_b_path, orig);

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
 * Copy the header of the start_lsn segment into the stop_lsn segment.
 * The stop_lsn segment header will have the wrong xlp_pageaddr.
 * check_wal_headers should report a page address mismatch.
 * Skipped if start_lsn and stop_lsn fall in the same segment.
 */
START_TEST(test_pg_probackup_wal_headers_bad_pageaddr)
{
	extern BackupAdapter pg_probackup_adapter;
	char backup_path[PATH_MAX];
	char *wal_path;
	char seg_a_path[PATH_MAX], seg_b_path[PATH_MAX];
	BackupInfo *backup;
	WALArchiveInfo *wal_info;
	ValidationResult *result;
	WALSegmentName seg_a_info, seg_b_info;
	char seg_a_name[25], seg_b_name[25];
	uint8_t hdr_a[WAL_HDR_LEN], orig_b[WAL_HDR_LEN];

	INTEGRATION_SKIP_BACKUP();

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	backup = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(backup);

	lsn_to_seg(backup->start_lsn, backup->timeline, &seg_a_info, 0);
	lsn_to_seg(backup->stop_lsn,  backup->timeline, &seg_b_info, 0);
	format_wal_filename(&seg_a_info, seg_a_name, sizeof(seg_a_name));
	format_wal_filename(&seg_b_info, seg_b_name, sizeof(seg_b_name));

	if (seg_a_info.seg_id == seg_b_info.seg_id)
	{
		fprintf(stderr, "SKIP: backup spans only one WAL segment, need two distinct segments\n");
		free_backup_list(backup);
		return;
	}

	wal_path = pg_probackup_adapter.get_wal_archive_path(backup_path,
														 backup->instance_name);
	ck_assert_ptr_nonnull(wal_path);

	snprintf(seg_a_path, sizeof(seg_a_path), "%s/%s", wal_path, seg_a_name);
	snprintf(seg_b_path, sizeof(seg_b_path), "%s/%s", wal_path, seg_b_name);

	ck_assert(read_seg_header(seg_a_path, hdr_a));
	ck_assert(read_seg_header(seg_b_path, orig_b));

	/* Overwrite seg_b header with seg_a header (wrong xlp_pageaddr) */
	ck_assert(write_seg_header(seg_b_path, hdr_a));

	wal_info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(wal_info);

	result = check_wal_headers(backup, wal_info);

	/* Restore before asserting */
	write_seg_header(seg_b_path, orig_b);

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

	INTEGRATION_SKIP_BACKUP();

	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

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

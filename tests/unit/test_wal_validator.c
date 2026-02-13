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


#include <stdlib.h>
#include <string.h>
#include <check.h>
#include "../../include/types.h"
#include "../../include/common.h"

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

/*
 * Test Suite
 */
Suite*
wal_validator_suite(void)
{
	Suite *s;
	TCase *tc_availability;

	s = suite_create("WAL Validator");

	/* WAL availability tests */
	tc_availability = tcase_create("Availability");
	tcase_add_test(tc_availability, test_check_wal_availability_all_present);
	tcase_add_test(tc_availability, test_check_wal_availability_missing);
	tcase_add_test(tc_availability, test_check_wal_availability_no_lsn);
	tcase_add_test(tc_availability, test_check_wal_availability_single_segment);
	tcase_add_test(tc_availability, test_check_wal_availability_gap);
	tcase_add_test(tc_availability, test_check_wal_availability_empty_archive);
	suite_add_tcase(s, tc_availability);

	return s;
}

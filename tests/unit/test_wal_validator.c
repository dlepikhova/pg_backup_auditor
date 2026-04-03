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
#include <sys/stat.h>
#include <unistd.h>
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
 * redo_lsn tests: verify that check_wal_availability extends the WAL range
 * to lsn_to_seg(redo_lsn) when redo_lsn falls in an earlier segment than
 * start_lsn.  Tests work entirely in-memory (no disk files).
 *
 * Segment layout used by these tests (16 MB each):
 *   seg 1 = 0x1000000 .. 0x1FFFFFF
 *   seg 2 = 0x2000000 .. 0x2FFFFFF   ← start_lsn segment
 *   seg 3 = 0x3000000 .. 0x3FFFFFF
 *   ...
 *   seg 5 = 0x5000000 .. 0x5FFFFFF   ← stop_lsn segment
 * ----------------------------------------------------------------------- */

/*
 * Helper: build a WALArchiveInfo with segments [first_seg, last_seg] inclusive.
 * timeline=1, log_id=0 throughout.
 */
static WALArchiveInfo *
make_segs(int first_seg, int last_seg)
{
	WALArchiveInfo *wi;
	int             count = last_seg - first_seg + 1;
	int             i;

	wi = calloc(1, sizeof(WALArchiveInfo));
	ck_assert_ptr_nonnull(wi);
	strcpy(wi->archive_path, "/test/redo/archive");
	wi->segment_count = count;
	wi->segments = malloc((size_t)count * sizeof(WALSegmentName));
	ck_assert_ptr_nonnull(wi->segments);

	for (i = 0; i < count; i++)
	{
		wi->segments[i].timeline = 1;
		wi->segments[i].log_id  = 0;
		wi->segments[i].seg_id  = (uint32_t)(first_seg + i);
	}
	return wi;
}

/*
 * redo_lsn within the same WAL segment as start_lsn → no range extension.
 * Archive has only segments 2..5.  redo_lsn=0x2000100 → still segment 2.
 * Expected: 0 errors.
 */
START_TEST(test_wal_avail_redo_same_segment)
{
	BackupInfo      bi;
	WALArchiveInfo *wi;
	ValidationResult *r;

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "redo-same-seg");
	bi.timeline  = 1;
	bi.start_lsn = 0x2000000;   /* segment 2 */
	bi.stop_lsn  = 0x5000000;   /* segment 5 */
	bi.redo_lsn  = 0x2000100;   /* still segment 2, just a different offset */

	wi = make_segs(2, 5);       /* segments 2..5 present, no seg 1 */

	r = check_wal_availability(&bi, wi);
	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	ck_assert_int_eq(r->status, BACKUP_STATUS_OK);

	free_validation_result(r);
	free_wal_archive_info(wi);
}
END_TEST

/*
 * redo_lsn in an earlier segment (seg 1) and that segment IS in the archive.
 * Expected: 0 errors.
 */
START_TEST(test_wal_avail_redo_earlier_present)
{
	BackupInfo      bi;
	WALArchiveInfo *wi;
	ValidationResult *r;

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "redo-earlier-ok");
	bi.timeline  = 1;
	bi.start_lsn = 0x2000000;   /* segment 2 */
	bi.stop_lsn  = 0x5000000;   /* segment 5 */
	bi.redo_lsn  = 0x1000000;   /* segment 1 — earlier than start_lsn segment */

	wi = make_segs(1, 5);       /* segments 1..5 all present */

	r = check_wal_availability(&bi, wi);
	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	ck_assert_int_eq(r->status, BACKUP_STATUS_OK);

	free_validation_result(r);
	free_wal_archive_info(wi);
}
END_TEST

/*
 * redo_lsn in an earlier segment (seg 1) but that segment is MISSING from
 * the archive.  Without the fix, this would report 0 errors.  With the fix,
 * the range is extended to seg 1 → 1 error (missing seg 1).
 */
START_TEST(test_wal_avail_redo_earlier_missing)
{
	BackupInfo      bi;
	WALArchiveInfo *wi;
	ValidationResult *r;

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "redo-earlier-miss");
	bi.timeline  = 1;
	bi.start_lsn = 0x2000000;   /* segment 2 */
	bi.stop_lsn  = 0x5000000;   /* segment 5 */
	bi.redo_lsn  = 0x1000000;   /* segment 1 — earlier, and NOT in archive */

	wi = make_segs(2, 5);       /* segments 2..5 only — seg 1 absent */

	r = check_wal_availability(&bi, wi);
	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert_int_eq(r->status, BACKUP_STATUS_ERROR);

	free_validation_result(r);
	free_wal_archive_info(wi);
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

/* ------------------------------------------------------------------ *
 * Per-record CRC unit tests
 *
 * These tests call check_wal_headers() with a synthetic WAL segment
 * written to /tmp.  No real backup catalog is required.
 *
 * Segment used: timeline=1, start_lsn=stop_lsn=0x1000000 → file name
 * 000000010000000000000001, seg_size=16MB, blcksz=8192.
 * ------------------------------------------------------------------ */

/*
 * Minimal CRC32C (Castagnoli) implementation for computing expected
 * checksums in test records.  Mirrors the algorithm in wal_validator.c.
 */
static uint32_t
test_crc32c(const uint8_t *buf, size_t len)
{
	static const uint32_t poly = 0x82F63B78U;
	uint32_t crc = ~0U;
	size_t i;
	int j;

	for (i = 0; i < len; i++)
	{
		crc ^= buf[i];
		for (j = 0; j < 8; j++)
			crc = (crc & 1) ? ((crc >> 1) ^ poly) : (crc >> 1);
	}
	return ~crc;
}

/*
 * Compute xl_crc for an XLogRecord following PostgreSQL's order:
 *   1. payload bytes [24 .. xl_tot_len-1]
 *   2. header bytes  [0  .. 19]
 */
static uint32_t
test_xlog_crc(const uint8_t *rec, uint32_t xl_tot_len)
{
	static const uint32_t poly = 0x82F63B78U;
	uint32_t crc = ~0U;
	uint32_t i;
	int j;

#define CRC_BYTE(b) do { \
		crc ^= (b); \
		for (j = 0; j < 8; j++) \
			crc = (crc & 1) ? ((crc >> 1) ^ poly) : (crc >> 1); \
	} while (0)

	/* 1. payload */
	for (i = 24; i < xl_tot_len; i++)
		CRC_BYTE(rec[i]);

	/* 2. header [0..19] */
	for (i = 0; i < 20; i++)
		CRC_BYTE(rec[i]);

#undef CRC_BYTE
	return ~crc;
}

/*
 * Write a WAL segment file to `path` consisting of a 40-byte long page
 * header followed by `nrecs` minimal 24-byte XLogRecord entries.
 *
 * All header fields are chosen so that validate_wal_segment_header() passes
 * (magic=0xD071, XLP_LONG_HEADER set, tli=1, pageaddr=0x1000000, etc.).
 *
 * For each record the CRC covers the 20 header bytes before xl_crc
 * (bytes [0..19]).  If bad_crc_idx >= 0, the CRC of that record is
 * intentionally corrupted with XOR 0xDEADBEEF.
 *
 * If zero_crc is true all xl_crc fields are written as 0 (synthetic WAL).
 *
 * If oversized is true, the first record's xl_tot_len is set to blcksz
 * (8192), making it a multi-page record that must be skipped.
 *
 * bad_totlen_idx: if >= 0, that record's xl_tot_len is set to 10 (< 24),
 * triggering the invalid-totlen path in validate_wal_segment_records.
 * Set bad_totlen_idx > 0 so that record[0] (checked by validate_wal_segment_header)
 * remains valid, allowing validate_wal_segment_records to be reached.
 */
static void
write_rec_test_seg(const char *path, int nrecs, int bad_crc_idx, int bad_totlen_idx,
				   bool zero_crc, bool oversized)
{
	FILE    *f;
	uint8_t  hdr[40] = {0};
	int      i;

	/* --- long page header --- */
	/* xlp_magic = 0xD071 (LE) */
	hdr[0] = 0x71; hdr[1] = 0xD0;
	/* xlp_info = 0x0002 (XLP_LONG_HEADER, LE) */
	hdr[2] = 0x02; hdr[3] = 0x00;
	/* xlp_tli = 1 (LE) */
	hdr[4] = 0x01; hdr[5] = 0x00; hdr[6] = 0x00; hdr[7] = 0x00;
	/* xlp_pageaddr = 0x0000000001000000 (LE) — start of segment 1 */
	hdr[8]  = 0x00; hdr[9]  = 0x00; hdr[10] = 0x00; hdr[11] = 0x01;
	hdr[12] = 0x00; hdr[13] = 0x00; hdr[14] = 0x00; hdr[15] = 0x00;
	/* xlp_rem_len = 0 */
	/* xlp_sysid = 1 (LE) */
	hdr[24] = 0x01;
	/* xlp_seg_size = 0x01000000 = 16 MB (LE) */
	hdr[32] = 0x00; hdr[33] = 0x00; hdr[34] = 0x00; hdr[35] = 0x01;
	/* xlp_xlog_blcksz = 8192 = 0x00002000 (LE) */
	hdr[36] = 0x00; hdr[37] = 0x20; hdr[38] = 0x00; hdr[39] = 0x00;

	f = fopen(path, "wb");
	if (f == NULL)
		return;

	fwrite(hdr, 1, sizeof(hdr), f);

	for (i = 0; i < nrecs; i++)
	{
		uint8_t  rec[24] = {0};
		uint32_t tot_len;
		uint32_t crc_val;

		/* xl_tot_len at offset 0 */
		if (oversized && i == 0)
			tot_len = 8192;	  /* larger than a full page — multi-page record */
		else if (bad_totlen_idx >= 0 && i == bad_totlen_idx)
			tot_len = 10;	  /* less than sizeof(XLogRecord) — invalid */
		else
			tot_len = 24;	  /* minimal header-only record */

		rec[0] = (uint8_t)(tot_len & 0xFF);
		rec[1] = (uint8_t)((tot_len >> 8) & 0xFF);
		rec[2] = (uint8_t)((tot_len >> 16) & 0xFF);
		rec[3] = (uint8_t)((tot_len >> 24) & 0xFF);

		/* xl_crc at offset 20 — PostgreSQL order: payload then header */
		if (zero_crc || tot_len > (uint32_t) sizeof(rec))
		{
			/*
			 * For oversized records (tot_len > sizeof(rec)) we can't compute
			 * a real CRC without a matching payload buffer.  The validator
			 * skips multi-page records anyway, so zero is fine.
			 */
			crc_val = 0;
		}
		else
		{
			crc_val = test_xlog_crc(rec, tot_len);  /* rec[20..23] still 0 */
			if (i == bad_crc_idx)
				crc_val ^= 0xDEADBEEF;
		}

		rec[20] = (uint8_t)(crc_val & 0xFF);
		rec[21] = (uint8_t)((crc_val >> 8) & 0xFF);
		rec[22] = (uint8_t)((crc_val >> 16) & 0xFF);
		rec[23] = (uint8_t)((crc_val >> 24) & 0xFF);

		fwrite(rec, 1, sizeof(rec), f);
	}

	/*
	 * Extend the file to the full segment size (16 MB) declared in the header.
	 * A seek to the last byte + one-byte write creates a sparse file on APFS /
	 * ext4, so no 16 MB of disk is actually allocated.  This satisfies the
	 * segment-size check in validate_wal_segment_header().
	 */
	(void)fseek(f, (long)0x1000000 - 1, SEEK_SET);
	(void)fputc('\0', f);

	fclose(f);
}

/*
 * Common setup: create the archive dir in /tmp and return BackupInfo +
 * WALArchiveInfo ready to be passed to check_wal_headers().
 */
static void
rec_test_setup(char *dir_out, size_t dir_sz, BackupInfo *bi, WALArchiveInfo *wi)
{
	snprintf(dir_out, dir_sz, "/tmp/pg_wrcrc_%d", (int)getpid());
	mkdir(dir_out, 0755);

	memset(bi, 0, sizeof(*bi));
	bi->timeline  = 1;
	bi->start_lsn = 0x1000000;   /* segment 1 */
	bi->stop_lsn  = 0x1000000;
	strncpy(bi->backup_id, "RECTEST", sizeof(bi->backup_id) - 1);

	memset(wi, 0, sizeof(*wi));
	strncpy(wi->archive_path, dir_out, sizeof(wi->archive_path) - 1);
}

static void
rec_test_teardown(const char *dir)
{
	char cmd[PATH_MAX + 8];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);
}

/* All three records have correct CRCs → no errors */
START_TEST(test_rec_crc_all_valid)
{
	char           dir[64];
	char           seg[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo wi;
	ValidationResult *r;

	rec_test_setup(dir, sizeof(dir), &bi, &wi);
	snprintf(seg, sizeof(seg), "%s/000000010000000000000001", dir);
	write_rec_test_seg(seg, 3, -1, -1, false, false);

	r = check_wal_headers(&bi, &wi);
	rec_test_teardown(dir);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/* Record [1] has a corrupted CRC → exactly 1 error containing "CRC mismatch" */
START_TEST(test_rec_crc_mismatch)
{
	char           dir[64];
	char           seg[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo wi;
	ValidationResult *r;

	rec_test_setup(dir, sizeof(dir), &bi, &wi);
	snprintf(seg, sizeof(seg), "%s/000000010000000000000001", dir);
	write_rec_test_seg(seg, 3, 1, -1, false, false);

	r = check_wal_headers(&bi, &wi);
	rec_test_teardown(dir);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "CRC mismatch") != NULL);
	free_validation_result(r);
}
END_TEST

/* Record [1] has xl_tot_len=10 (<24) → 1 error containing "invalid xl_tot_len".
 * Record [0] must be valid so validate_wal_segment_header() passes and
 * validate_wal_segment_records() is actually reached. */
START_TEST(test_rec_crc_invalid_totlen)
{
	char           dir[64];
	char           seg[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo wi;
	ValidationResult *r;

	rec_test_setup(dir, sizeof(dir), &bi, &wi);
	snprintf(seg, sizeof(seg), "%s/000000010000000000000001", dir);
	write_rec_test_seg(seg, 2, -1, 1, false, false);

	r = check_wal_headers(&bi, &wi);
	rec_test_teardown(dir);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "invalid xl_tot_len") != NULL);
	free_validation_result(r);
}
END_TEST

/* Records with xl_crc=0 (synthetic WAL) → no errors */
START_TEST(test_rec_crc_zero_skipped)
{
	char           dir[64];
	char           seg[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo wi;
	ValidationResult *r;

	rec_test_setup(dir, sizeof(dir), &bi, &wi);
	snprintf(seg, sizeof(seg), "%s/000000010000000000000001", dir);
	write_rec_test_seg(seg, 3, -1, -1, true, false);

	r = check_wal_headers(&bi, &wi);
	rec_test_teardown(dir);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/* Record with xl_tot_len > page remainder → skipped silently, no error */
START_TEST(test_rec_crc_multipage_skipped)
{
	char           dir[64];
	char           seg[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo wi;
	ValidationResult *r;

	rec_test_setup(dir, sizeof(dir), &bi, &wi);
	snprintf(seg, sizeof(seg), "%s/000000010000000000000001", dir);
	write_rec_test_seg(seg, 1, -1, -1, false, true);

	r = check_wal_headers(&bi, &wi);
	rec_test_teardown(dir);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/* -------------------------------------------------------------------------
 * CRC-with-payload tests
 *
 * Records with xl_tot_len > 24 have a non-empty payload.  The CRC must be
 * computed in PostgreSQL order: payload first, then the 20-byte header.
 * These tests verify that the validator uses the correct order.
 * ------------------------------------------------------------------------- */

/*
 * Write a segment containing one record with `payload_len` bytes of payload
 * (all 0xAB), optionally corrupted.
 */
static void
write_payload_seg(const char *path, uint32_t payload_len, bool corrupt_crc)
{
	FILE    *f;
	uint8_t  hdr[40] = {0};
	uint32_t tot_len = 24 + payload_len;
	uint32_t crc_val;
	uint8_t *rec;

	/* page header */
	hdr[0] = 0x71; hdr[1] = 0xD0;          /* xlp_magic */
	hdr[2] = 0x02; hdr[3] = 0x00;          /* XLP_LONG_HEADER */
	hdr[4] = 0x01;                          /* xlp_tli = 1 */
	hdr[8]  = 0x00; hdr[9]  = 0x00;
	hdr[10] = 0x00; hdr[11] = 0x01;        /* xlp_pageaddr = 0x1000000 */
	hdr[24] = 0x01;                         /* xlp_sysid = 1 */
	hdr[32] = 0x00; hdr[33] = 0x00;
	hdr[34] = 0x00; hdr[35] = 0x01;        /* xlp_seg_size = 16 MB */
	hdr[36] = 0x00; hdr[37] = 0x20;        /* xlp_xlog_blcksz = 8192 */

	rec = calloc(1, tot_len);
	if (rec == NULL) return;

	rec[0] = (uint8_t)(tot_len & 0xFF);
	rec[1] = (uint8_t)((tot_len >> 8) & 0xFF);

	/* fill payload with 0xAB */
	memset(rec + 24, 0xAB, payload_len);

	crc_val = test_xlog_crc(rec, tot_len);
	if (corrupt_crc)
		crc_val ^= 0xDEADBEEF;

	rec[20] = (uint8_t)(crc_val & 0xFF);
	rec[21] = (uint8_t)((crc_val >> 8) & 0xFF);
	rec[22] = (uint8_t)((crc_val >> 16) & 0xFF);
	rec[23] = (uint8_t)((crc_val >> 24) & 0xFF);

	f = fopen(path, "wb");
	if (f == NULL) { free(rec); return; }

	fwrite(hdr, 1, sizeof(hdr), f);
	fwrite(rec, 1, tot_len, f);
	free(rec);

	/* extend to full 16 MB */
	(void)fseek(f, (long)0x1000000 - 1, SEEK_SET);
	(void)fputc('\0', f);
	fclose(f);
}

/* Record with 6-byte payload and correct CRC → no errors */
START_TEST(test_rec_crc_payload_valid)
{
	char           dir[64];
	char           seg[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo wi;
	ValidationResult *r;

	rec_test_setup(dir, sizeof(dir), &bi, &wi);
	snprintf(seg, sizeof(seg), "%s/000000010000000000000001", dir);
	write_payload_seg(seg, 6, false);

	r = check_wal_headers(&bi, &wi);
	rec_test_teardown(dir);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/* Record with 6-byte payload and corrupted CRC → 1 error */
START_TEST(test_rec_crc_payload_mismatch)
{
	char           dir[64];
	char           seg[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo wi;
	ValidationResult *r;

	rec_test_setup(dir, sizeof(dir), &bi, &wi);
	snprintf(seg, sizeof(seg), "%s/000000010000000000000001", dir);
	write_payload_seg(seg, 6, true);

	r = check_wal_headers(&bi, &wi);
	rec_test_teardown(dir);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_ge(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "CRC mismatch") != NULL);
	free_validation_result(r);
}
END_TEST

/* -------------------------------------------------------------------------
 * Tests for check_wal_archive_headers()
 *
 * These tests exercise the archive-wide header check introduced to fix
 * BUG-003: segments that fall between backups are never reached by
 * check_wal_headers(), so a segment swap among them goes undetected.
 *
 * check_wal_archive_headers() iterates every WALSegmentName in the
 * WALArchiveInfo and validates each file's xlp_pageaddr against the
 * segment number.
 *
 * Helper write_arch_test_seg() writes a 40-byte long page header with a
 * configurable timeline and pageaddr, followed by one valid 24-byte record.
 * ------------------------------------------------------------------------- */

/*
 * Write a minimal WAL segment to `path` with the given timeline and
 * page address.  Used to test archive-wide header validation.
 */
static void
write_arch_test_seg(const char *path, uint32_t tli, uint64_t pageaddr)
{
	FILE    *f;
	uint8_t  hdr[40] = {0};
	uint8_t  rec[24] = {0};
	uint32_t crc_val;

	/* xlp_magic = 0xD071 (LE) */
	hdr[0] = 0x71; hdr[1] = 0xD0;
	/* xlp_info = XLP_LONG_HEADER (LE) */
	hdr[2] = 0x02; hdr[3] = 0x00;
	/* xlp_tli (LE) */
	hdr[4] = (uint8_t)(tli);
	hdr[5] = (uint8_t)(tli >> 8);
	hdr[6] = (uint8_t)(tli >> 16);
	hdr[7] = (uint8_t)(tli >> 24);
	/* xlp_pageaddr (LE) */
	hdr[8]  = (uint8_t)(pageaddr);
	hdr[9]  = (uint8_t)(pageaddr >> 8);
	hdr[10] = (uint8_t)(pageaddr >> 16);
	hdr[11] = (uint8_t)(pageaddr >> 24);
	hdr[12] = (uint8_t)(pageaddr >> 32);
	hdr[13] = (uint8_t)(pageaddr >> 40);
	hdr[14] = (uint8_t)(pageaddr >> 48);
	hdr[15] = (uint8_t)(pageaddr >> 56);
	/* xlp_sysid = 1 */
	hdr[24] = 0x01;
	/* xlp_seg_size = 16 MB (LE) */
	hdr[32] = 0x00; hdr[33] = 0x00; hdr[34] = 0x00; hdr[35] = 0x01;
	/* xlp_xlog_blcksz = 8192 (LE) */
	hdr[36] = 0x00; hdr[37] = 0x20; hdr[38] = 0x00; hdr[39] = 0x00;

	f = fopen(path, "wb");
	if (f == NULL)
		return;
	fwrite(hdr, 1, sizeof(hdr), f);

	/* One valid 24-byte XLogRecord */
	rec[0] = 24;	/* xl_tot_len */
	crc_val = test_crc32c(rec, 20);
	rec[20] = (uint8_t)(crc_val);
	rec[21] = (uint8_t)(crc_val >> 8);
	rec[22] = (uint8_t)(crc_val >> 16);
	rec[23] = (uint8_t)(crc_val >> 24);
	fwrite(rec, 1, sizeof(rec), f);

	/* Extend to full 16 MB (sparse file: no actual disk allocation for gap) */
	(void)fseek(f, (long)0x1000000 - 1, SEEK_SET);
	(void)fputc('\0', f);

	fclose(f);
}

/* NULL input → returns NULL */
START_TEST(test_archive_headers_null)
{
	ValidationResult *r = check_wal_archive_headers(NULL);
	ck_assert_ptr_null(r);
}
END_TEST

/* Empty archive (0 segments) → OK, no errors */
START_TEST(test_archive_headers_empty)
{
	char           dir[64];
	WALArchiveInfo wi;
	ValidationResult *r;

	snprintf(dir, sizeof(dir), "/tmp/pg_warch_%d", (int)getpid());
	mkdir(dir, 0755);
	memset(&wi, 0, sizeof(wi));
	strncpy(wi.archive_path, dir, sizeof(wi.archive_path) - 1);
	wi.segment_count = 0;
	wi.segments = NULL;

	r = check_wal_archive_headers(&wi);

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/* Three segments with correct headers → 0 errors */
START_TEST(test_archive_headers_all_valid)
{
	char           dir[64];
	char           seg[PATH_MAX];
	WALArchiveInfo wi;
	ValidationResult *r;
	WALSegmentName segs[3];

	snprintf(dir, sizeof(dir), "/tmp/pg_warch_%d", (int)getpid());
	mkdir(dir, 0755);

	/* Segments 1, 2, 3 on timeline 1 */
	for (int i = 0; i < 3; i++)
	{
		segs[i].timeline = 1;
		segs[i].log_id   = 0;
		segs[i].seg_id   = (uint32_t)(i + 1);

		uint64_t pa = (uint64_t)(i + 1) * 0x1000000ULL;
		snprintf(seg, sizeof(seg),
				 "%s/0000000100000000%08X", dir, i + 1);
		write_arch_test_seg(seg, 1, pa);
	}

	memset(&wi, 0, sizeof(wi));
	strncpy(wi.archive_path, dir, sizeof(wi.archive_path) - 1);
	wi.segment_count = 3;
	wi.segments = segs;

	r = check_wal_archive_headers(&wi);
	wi.segments = NULL;	/* stack-allocated; don't free */

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * Segment file "000000010000000000000004" contains a header claiming
 * xlp_pageaddr = 0x1000000 (segment 1's address).  This simulates a
 * segment swap — the corruption that BUG-003 was about.
 * Expected: exactly 1 error containing "page address mismatch".
 */
START_TEST(test_archive_headers_bad_pageaddr)
{
	char           dir[64];
	char           seg[PATH_MAX];
	WALArchiveInfo wi;
	ValidationResult *r;
	WALSegmentName segs[1];

	snprintf(dir, sizeof(dir), "/tmp/pg_warch_%d", (int)getpid());
	mkdir(dir, 0755);

	/* File named segment 4 but header says pageaddr of segment 1 */
	segs[0].timeline = 1;
	segs[0].log_id   = 0;
	segs[0].seg_id   = 4;

	snprintf(seg, sizeof(seg), "%s/000000010000000000000004", dir);
	write_arch_test_seg(seg, 1, 0x1000000ULL);	/* wrong: should be 0x4000000 */

	memset(&wi, 0, sizeof(wi));
	strncpy(wi.archive_path, dir, sizeof(wi.archive_path) - 1);
	wi.segment_count = 1;
	wi.segments = segs;

	r = check_wal_archive_headers(&wi);
	wi.segments = NULL;

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "page address mismatch") != NULL);
	free_validation_result(r);
}
END_TEST

/*
 * Segment file has a valid 40-byte header (xlp_seg_size=16MB) but the file
 * is only 64 bytes.  Expected: 1 error containing "truncated".
 */
START_TEST(test_archive_headers_truncated)
{
	char           dir[64];
	char           seg_path[PATH_MAX];
	WALArchiveInfo wi;
	ValidationResult *r;
	WALSegmentName segs[1];
	FILE          *f;
	uint8_t        hdr[40] = {0};

	snprintf(dir, sizeof(dir), "/tmp/pg_warch_%d", (int)getpid());
	mkdir(dir, 0755);

	segs[0].timeline = 1;
	segs[0].log_id   = 0;
	segs[0].seg_id   = 1;

	snprintf(seg_path, sizeof(seg_path), "%s/000000010000000000000001", dir);

	/* Write a valid header with xlp_seg_size=16MB, but do NOT extend to 16MB */
	hdr[0] = 0x71; hdr[1] = 0xD0;          /* xlp_magic */
	hdr[2] = 0x02; hdr[3] = 0x00;          /* xlp_info = XLP_LONG_HEADER */
	hdr[4] = 0x01;                          /* xlp_tli = 1 */
	hdr[8] = 0x00; hdr[9] = 0x00;          /* xlp_pageaddr = 0x1000000 */
	hdr[10] = 0x00; hdr[11] = 0x01;
	hdr[24] = 0x01;                         /* xlp_sysid = 1 */
	hdr[32] = 0x00; hdr[33] = 0x00;        /* xlp_seg_size = 16MB */
	hdr[34] = 0x00; hdr[35] = 0x01;
	hdr[36] = 0x00; hdr[37] = 0x20;        /* xlp_xlog_blcksz = 8192 */

	f = fopen(seg_path, "wb");
	ck_assert_ptr_nonnull(f);
	fwrite(hdr, 1, sizeof(hdr), f);
	fclose(f);                              /* file is 40 bytes — truncated */

	memset(&wi, 0, sizeof(wi));
	strncpy(wi.archive_path, dir, sizeof(wi.archive_path) - 1);
	wi.segment_count = 1;
	wi.segments = segs;

	r = check_wal_archive_headers(&wi);
	wi.segments = NULL;

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "truncated") != NULL);
	free_validation_result(r);
}
END_TEST

/* -------------------------------------------------------------------------
 * Tests for check_wal_restore_chain()
 *
 * check_wal_restore_chain() verifies that WAL is continuously available
 * between every consecutive pair of backups on the same timeline.
 * The "bridge" between backup A and backup B is the set of WAL segments
 * in the range [lsn_to_seg(A.stop_lsn)+1, lsn_to_seg(B.start_lsn)-1].
 * If any bridge segment is absent from the archive an error is reported.
 *
 * These tests use in-memory WALArchiveInfo (no real files needed —
 * segment_exists_in_archive() checks the segments[] array, not the fs).
 * BackupInfo nodes are heap-allocated and freed by free_chain() below.
 * ------------------------------------------------------------------------- */

/* Helper: build a heap-allocated BackupInfo linked list node. */
static BackupInfo *
make_backup_node(const char *id, uint32_t tli,
				 uint64_t start_lsn, uint64_t stop_lsn,
				 BackupInfo *next)
{
	BackupInfo *b = calloc(1, sizeof(BackupInfo));

	strncpy(b->backup_id, id, sizeof(b->backup_id) - 1);
	b->timeline  = tli;
	b->start_lsn = start_lsn;
	b->stop_lsn  = stop_lsn;
	b->status    = BACKUP_STATUS_OK;
	b->next      = next;
	return b;
}

/* Helper: free a BackupInfo linked list. */
static void
free_chain(BackupInfo *head)
{
	while (head)
	{
		BackupInfo *next = head->next;
		free(head);
		head = next;
	}
}

/*
 * Helper: build an in-memory WALArchiveInfo for the given segment IDs on
 * timeline tli.  segments[] is heap-allocated; caller frees it or passes
 * NULL archive_path (unused by check_wal_restore_chain — it only consults
 * the segments[] array via segment_exists_in_archive()).
 */
static void
build_archive(WALArchiveInfo *wi, uint32_t tli,
			  const uint32_t *seg_ids, int nseg)
{
	memset(wi, 0, sizeof(*wi));
	wi->segment_count = nseg;
	wi->segments      = calloc((size_t)nseg, sizeof(WALSegmentName));
	for (int i = 0; i < nseg; i++)
	{
		wi->segments[i].timeline = tli;
		wi->segments[i].log_id   = 0;
		wi->segments[i].seg_id   = seg_ids[i];
	}
}

/* NULL backups → returns NULL */
START_TEST(test_chain_null_backups)
{
	WALArchiveInfo wi;
	build_archive(&wi, 1, NULL, 0);

	ValidationResult *r = check_wal_restore_chain(NULL, &wi);
	free(wi.segments);
	ck_assert_ptr_null(r);
}
END_TEST

/* NULL archive → returns NULL */
START_TEST(test_chain_null_archive)
{
	BackupInfo *b = make_backup_node("B1", 1, 0x1000000, 0x3000000, NULL);

	ValidationResult *r = check_wal_restore_chain(b, NULL);
	free_chain(b);
	ck_assert_ptr_null(r);
}
END_TEST

/* Single backup — no pair to check → OK */
START_TEST(test_chain_single_backup)
{
	BackupInfo     *b = make_backup_node("B1", 1, 0x1000000, 0x3000000, NULL);
	WALArchiveInfo  wi;
	const uint32_t  segs[] = {1, 2, 3};
	build_archive(&wi, 1, segs, 3);

	ValidationResult *r = check_wal_restore_chain(b, &wi);
	free(wi.segments);
	free_chain(b);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * Two backups, adjacent: B1 ends at seg 3, B2 starts at seg 4.
 * Bridge = [4, 3] = empty → no missing segments, OK.
 */
START_TEST(test_chain_adjacent_backups)
{
	/* B2 is the newer backup (higher LSN); linked list order doesn't matter
	 * because check_wal_restore_chain sorts internally. */
	BackupInfo     *b2 = make_backup_node("B2", 1, 0x4000000, 0x6000000, NULL);
	BackupInfo     *b1 = make_backup_node("B1", 1, 0x1000000, 0x3000000, b2);
	WALArchiveInfo  wi;
	const uint32_t  segs[] = {1, 2, 3, 4, 5, 6};
	build_archive(&wi, 1, segs, 6);

	ValidationResult *r = check_wal_restore_chain(b1, &wi);
	free(wi.segments);
	free_chain(b1);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * B1 is OK, B2 is ORPHAN.  Archive is missing bridge segments 4-7.
 * B2 must be excluded from the chain check → 0 errors.
 */
START_TEST(test_chain_orphan_skipped)
{
	BackupInfo     *b2 = make_backup_node("B2", 1, 0x8000000, 0xA000000, NULL);
	BackupInfo     *b1 = make_backup_node("B1", 1, 0x1000000, 0x3000000, b2);
	WALArchiveInfo  wi;
	/* Bridge segments 4-7 absent — would produce 4 errors if B2 were included */
	const uint32_t  segs[] = {1, 2, 3, 8, 9, 10};

	b2->status = BACKUP_STATUS_ORPHAN;
	build_archive(&wi, 1, segs, 6);

	ValidationResult *r = check_wal_restore_chain(b1, &wi);
	free(wi.segments);
	free_chain(b1);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * Two backups with a gap between them; all bridge segments present.
 * B1: segs 1-3  B2: segs 8-10  Bridge: segs 4-7 (all in archive) → OK.
 */
START_TEST(test_chain_complete_bridge)
{
	BackupInfo     *b2 = make_backup_node("B2", 1, 0x8000000, 0xA000000, NULL);
	BackupInfo     *b1 = make_backup_node("B1", 1, 0x1000000, 0x3000000, b2);
	WALArchiveInfo  wi;
	const uint32_t  segs[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
	build_archive(&wi, 1, segs, 10);

	ValidationResult *r = check_wal_restore_chain(b1, &wi);
	free(wi.segments);
	free_chain(b1);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * Same setup but segments 5 and 6 are missing from the archive.
 * Expected: 2 errors, each mentioning "chain broken" and the backup IDs.
 */
START_TEST(test_chain_missing_bridge)
{
	BackupInfo     *b2 = make_backup_node("B2", 1, 0x8000000, 0xA000000, NULL);
	BackupInfo     *b1 = make_backup_node("B1", 1, 0x1000000, 0x3000000, b2);
	WALArchiveInfo  wi;
	/* Archive missing segments 5 and 6 */
	const uint32_t  segs[] = {1, 2, 3, 4, 7, 8, 9, 10};
	build_archive(&wi, 1, segs, 8);

	ValidationResult *r = check_wal_restore_chain(b1, &wi);
	free(wi.segments);
	free_chain(b1);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 2);
	ck_assert(strstr(r->errors[0], "chain broken") != NULL);
	ck_assert(strstr(r->errors[0], "B1") != NULL);
	ck_assert(strstr(r->errors[0], "B2") != NULL);
	free_validation_result(r);
}
END_TEST

/*
 * Two backups on different timelines; no history file in the archive.
 * check_wal_restore_chain() now attempts to read the history file to find
 * the switch LSN.  Without the file it reports 1 error ("cross-timeline …
 * missing or entry … not found").
 */
START_TEST(test_chain_different_timelines)
{
	BackupInfo     *b2 = make_backup_node("B2", 2, 0x1000000, 0x3000000, NULL);
	BackupInfo     *b1 = make_backup_node("B1", 1, 0x1000000, 0x3000000, b2);
	WALArchiveInfo  wi;
	build_archive(&wi, 1, NULL, 0);	/* archive_path is empty — no history file */

	ValidationResult *r = check_wal_restore_chain(b1, &wi);
	free(wi.segments);
	free_chain(b1);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "cross-timeline") != NULL);
	free_validation_result(r);
}
END_TEST

/* -------------------------------------------------------------------------
 * Tests for cross-timeline WAL chain validation
 *
 * When backup A is on tli=1 and backup B is on tli=2, recovery from A to B
 * requires:
 *   1. WAL on tli=1 from A.stop_lsn to the timeline switch point
 *      (pre-switch bridge).
 *   2. WAL on tli=2 from the switch point to B.start_lsn
 *      (post-switch bridge; may be empty).
 *
 * The switch LSN comes from the NNNNNNNN.history file in the archive.
 *
 * These tests write a real history file to a temp dir because
 * parse_history_switchpoint() reads from disk; segment presence is checked
 * via the in-memory segments[] array (no segment files needed).
 * ------------------------------------------------------------------------- */

/*
 * Helper: write a one-line timeline history file.
 *   dir/NNNNNNNN.history: "<parent_tli>\t<switch_lsn_str>\treason\n"
 */
static void
write_history_file(const char *dir, uint32_t new_tli, uint32_t parent_tli,
				   const char *switch_lsn_str)
{
	char  path[PATH_MAX];
	FILE *fp;

	snprintf(path, sizeof(path), "%s/%08X.history", dir, new_tli);
	fp = fopen(path, "w");
	if (fp == NULL)
		return;
	fprintf(fp, "%u\t%s\tno recovery target specified\n",
			parent_tli, switch_lsn_str);
	fclose(fp);
}

/*
 * Helper: build an in-memory WALArchiveInfo from an explicit WALSegmentName
 * array (may mix timelines).  archive_path is set so that parse_history_*
 * can find the history file on disk.
 */
static void
build_archive_from_segs(WALArchiveInfo *wi, const char *archive_path,
						const WALSegmentName *segs, int nseg)
{
	memset(wi, 0, sizeof(*wi));
	if (archive_path)
		str_copy(wi->archive_path, archive_path, sizeof(wi->archive_path));
	wi->segment_count = nseg;
	wi->segments = calloc((size_t)nseg, sizeof(WALSegmentName));
	if (wi->segments && nseg > 0)
		memcpy(wi->segments, segs, (size_t)nseg * sizeof(WALSegmentName));
}

/*
 * Scenario used by most cross-timeline tests:
 *   prev: tli=1, stop_lsn=0x2000000  → stop_seg  = tli=1 seg 2
 *   switch_lsn: 0x5000000            → pre-switch segs 3,4,5 on tli=1
 *   next: tli=2, start_lsn=0x8000000 → post-switch segs 6,7 on tli=2
 *                                       (switch_seg_new = tli=2 seg 5,
 *                                        next_start_seg = tli=2 seg 8,
 *                                        bridge = segs 6,7)
 */

/*
 * History file is absent → cannot determine switch point → 1 error.
 */
START_TEST(test_xchain_no_history_file)
{
	char           dir[64];
	BackupInfo    *next = make_backup_node("NEXT", 2, 0x8000000, 0xA000000, NULL);
	BackupInfo    *prev = make_backup_node("PREV", 1, 0x1000000, 0x2000000, next);
	WALArchiveInfo wi;

	snprintf(dir, sizeof(dir), "/tmp/pg_xch_%d", (int)getpid());
	mkdir(dir, 0755);
	/* No history file written */
	build_archive_from_segs(&wi, dir, NULL, 0);

	ValidationResult *r = check_wal_restore_chain(prev, &wi);

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);
	free(wi.segments);
	free_chain(prev);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "cross-timeline") != NULL);
	free_validation_result(r);
}
END_TEST

/*
 * History file present, all pre- and post-switch bridge segments in archive.
 * Expected: 0 errors.
 */
START_TEST(test_xchain_pre_switch_complete)
{
	char           dir[64];
	BackupInfo    *next = make_backup_node("NEXT", 2, 0x8000000, 0xA000000, NULL);
	BackupInfo    *prev = make_backup_node("PREV", 1, 0x1000000, 0x2000000, next);
	WALSegmentName segs[] = {
		/* tli=1, pre-switch bridge: segs 3,4,5 */
		{1, 0, 3}, {1, 0, 4}, {1, 0, 5},
		/* tli=2, post-switch bridge: segs 6,7 */
		{2, 0, 6}, {2, 0, 7}
	};
	WALArchiveInfo wi;

	snprintf(dir, sizeof(dir), "/tmp/pg_xch_%d", (int)getpid());
	mkdir(dir, 0755);
	write_history_file(dir, 2, 1, "0/5000000");
	build_archive_from_segs(&wi, dir, segs, 5);

	ValidationResult *r = check_wal_restore_chain(prev, &wi);

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);
	free(wi.segments);
	free_chain(prev);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * Segment 4 on tli=1 (pre-switch bridge) is missing from the archive.
 * Expected: 1 error mentioning "pre-switch".
 */
START_TEST(test_xchain_pre_switch_missing)
{
	char           dir[64];
	BackupInfo    *next = make_backup_node("NEXT", 2, 0x8000000, 0xA000000, NULL);
	BackupInfo    *prev = make_backup_node("PREV", 1, 0x1000000, 0x2000000, next);
	WALSegmentName segs[] = {
		{1, 0, 3}, /* 4 missing */ {1, 0, 5},
		{2, 0, 6}, {2, 0, 7}
	};
	WALArchiveInfo wi;

	snprintf(dir, sizeof(dir), "/tmp/pg_xch_%d", (int)getpid());
	mkdir(dir, 0755);
	write_history_file(dir, 2, 1, "0/5000000");
	build_archive_from_segs(&wi, dir, segs, 4);

	ValidationResult *r = check_wal_restore_chain(prev, &wi);

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);
	free(wi.segments);
	free_chain(prev);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "pre-switch") != NULL);
	free_validation_result(r);
}
END_TEST

/*
 * Segment 7 on tli=2 (post-switch bridge) is missing from the archive.
 * Expected: 1 error mentioning "post-switch".
 */
START_TEST(test_xchain_post_switch_missing)
{
	char           dir[64];
	BackupInfo    *next = make_backup_node("NEXT", 2, 0x8000000, 0xA000000, NULL);
	BackupInfo    *prev = make_backup_node("PREV", 1, 0x1000000, 0x2000000, next);
	WALSegmentName segs[] = {
		{1, 0, 3}, {1, 0, 4}, {1, 0, 5},
		{2, 0, 6} /* 7 missing */
	};
	WALArchiveInfo wi;

	snprintf(dir, sizeof(dir), "/tmp/pg_xch_%d", (int)getpid());
	mkdir(dir, 0755);
	write_history_file(dir, 2, 1, "0/5000000");
	build_archive_from_segs(&wi, dir, segs, 4);

	ValidationResult *r = check_wal_restore_chain(prev, &wi);

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);
	free(wi.segments);
	free_chain(prev);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	ck_assert(strstr(r->errors[0], "post-switch") != NULL);
	free_validation_result(r);
}
END_TEST

/*
 * Switch happens at exactly next.start_lsn → post-switch bridge is empty.
 * prev: tli=1, stop_lsn=0x4000000 (seg 4).
 * switch_lsn: 0x5000000 → switch_seg_old = tli=1 seg 5,
 *                          switch_seg_new = tli=2 seg 5.
 * next: tli=2, start_lsn=0x5000000 → next_start_seg = tli=2 seg 5.
 * Post-switch bridge: bridge_cur starts at seg 6, but 6 >= next_start 5 →
 * empty.  Pre-switch bridge: tli=1 seg 5 (stop+1 = 5 ≤ switch = 5).
 * Expected: 0 errors when tli=1 seg 5 is in archive.
 */
START_TEST(test_xchain_adjacent_to_switch)
{
	char           dir[64];
	BackupInfo    *next = make_backup_node("NEXT", 2, 0x5000000, 0x8000000, NULL);
	BackupInfo    *prev = make_backup_node("PREV", 1, 0x1000000, 0x4000000, next);
	WALSegmentName segs[] = {
		{1, 0, 5}   /* only pre-switch seg needed */
	};
	WALArchiveInfo wi;

	snprintf(dir, sizeof(dir), "/tmp/pg_xch_%d", (int)getpid());
	mkdir(dir, 0755);
	write_history_file(dir, 2, 1, "0/5000000");
	build_archive_from_segs(&wi, dir, segs, 1);

	ValidationResult *r = check_wal_restore_chain(prev, &wi);

	char cmd[80];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);
	free(wi.segments);
	free_chain(prev);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/* ======================================================================
 * Stream backup WAL validation tests
 *
 * pg_probackup stream backups store WAL in database/pg_wal/ inside the
 * backup directory.  We test check_wal_availability() using a real
 * on-disk directory scanned by scan_wal_archive().
 *
 * scan_wal_archive() only parses filenames, so empty stub files suffice.
 * Filename format: TTTTTTTTLLLLLLLLSSSSSSSS (8+8+8 hex digits).
 * ====================================================================== */

/*
 * Create an empty stub file with the standard WAL filename for the given
 * timeline / seg_id (log_id is always 0 in these tests).
 */
static void
create_wal_stub(const char *dir, uint32_t tli, uint32_t seg_id)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%08X%08X%08X", dir, tli, 0u, seg_id);
	FILE *fp = fopen(path, "w");
	if (fp != NULL) fclose(fp);
}

/*
 * All three required segments (1, 2, 3) are present in database/pg_wal/.
 * Expected: 0 errors.
 */
START_TEST(test_stream_wal_all_present)
{
	char           backup_dir[PATH_MAX];
	char           wal_dir[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo *wi;
	ValidationResult *r;

	snprintf(backup_dir, sizeof(backup_dir), "/tmp/pg_strm_%d", (int)getpid());
	snprintf(wal_dir,    sizeof(wal_dir),    "%s/database/pg_wal", backup_dir);
	mkdir(backup_dir, 0755);
	{
		char db_dir[PATH_MAX];
		snprintf(db_dir, sizeof(db_dir), "%s/database", backup_dir);
		mkdir(db_dir, 0755);
	}
	mkdir(wal_dir, 0755);

	create_wal_stub(wal_dir, 1, 1);
	create_wal_stub(wal_dir, 1, 2);
	create_wal_stub(wal_dir, 1, 3);

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "stream-all");
	bi.timeline   = 1;
	bi.start_lsn  = 0x1000000;   /* segment 1 */
	bi.stop_lsn   = 0x3000000;   /* segment 3 */
	bi.wal_stream = true;

	wi = scan_wal_archive(wal_dir);

	char cmd[PATH_MAX + 8];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", backup_dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(wi);
	r = check_wal_availability(&bi, wi);
	free_wal_archive_info(wi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * Segment 2 is missing from database/pg_wal/ (segments 1 and 3 present).
 * Expected: 1 error (missing segment 2).
 */
START_TEST(test_stream_wal_missing_segment)
{
	char           backup_dir[PATH_MAX];
	char           wal_dir[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo *wi;
	ValidationResult *r;

	snprintf(backup_dir, sizeof(backup_dir), "/tmp/pg_strm_%d", (int)getpid());
	snprintf(wal_dir,    sizeof(wal_dir),    "%s/database/pg_wal", backup_dir);
	mkdir(backup_dir, 0755);
	{
		char db_dir[PATH_MAX];
		snprintf(db_dir, sizeof(db_dir), "%s/database", backup_dir);
		mkdir(db_dir, 0755);
	}
	mkdir(wal_dir, 0755);

	create_wal_stub(wal_dir, 1, 1);
	/* segment 2 intentionally absent */
	create_wal_stub(wal_dir, 1, 3);

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "stream-miss");
	bi.timeline   = 1;
	bi.start_lsn  = 0x1000000;
	bi.stop_lsn   = 0x3000000;
	bi.wal_stream = true;

	wi = scan_wal_archive(wal_dir);

	char cmd[PATH_MAX + 8];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", backup_dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(wi);
	r = check_wal_availability(&bi, wi);
	free_wal_archive_info(wi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 1);
	free_validation_result(r);
}
END_TEST

/*
 * database/pg_wal/ exists but contains no WAL files.
 * Expected: 3 errors (segments 1, 2, 3 all missing).
 */
START_TEST(test_stream_wal_empty_dir)
{
	char           backup_dir[PATH_MAX];
	char           wal_dir[PATH_MAX];
	BackupInfo     bi;
	WALArchiveInfo *wi;
	ValidationResult *r;

	snprintf(backup_dir, sizeof(backup_dir), "/tmp/pg_strm_%d", (int)getpid());
	snprintf(wal_dir,    sizeof(wal_dir),    "%s/database/pg_wal", backup_dir);
	mkdir(backup_dir, 0755);
	{
		char db_dir[PATH_MAX];
		snprintf(db_dir, sizeof(db_dir), "%s/database", backup_dir);
		mkdir(db_dir, 0755);
	}
	mkdir(wal_dir, 0755);
	/* no stubs created — directory is empty */

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "stream-empty");
	bi.timeline   = 1;
	bi.start_lsn  = 0x1000000;
	bi.stop_lsn   = 0x3000000;
	bi.wal_stream = true;

	wi = scan_wal_archive(wal_dir);

	char cmd[PATH_MAX + 8];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", backup_dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(wi);
	r = check_wal_availability(&bi, wi);
	free_wal_archive_info(wi);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 3);
	free_validation_result(r);
}
END_TEST

/* ======================================================================
 * Non-standard WAL segment size (BUG-002 fix)
 *
 * PostgreSQL can be compiled with --with-wal-segsize=N (N in MB, power of 2).
 * The actual size is stored in xlp_seg_size (offset 32) of every WAL page
 * header.  Previously all lsn_to_seg() calls hardcoded 16 MB; now
 * detect_wal_segment_size() reads the real value from the first segment.
 *
 * Helper write_wal_seg_with_size() writes a 40-byte WAL long page header with
 * a configurable segment size, then extends the file to that full size as a
 * sparse file.  No XLogRecord data is needed because tests only exercise
 * check_wal_availability() and check_wal_headers() at the header level.
 * ====================================================================== */

/*
 * Write a minimal WAL long page header with configurable seg_size.
 * The file is extended to seg_size bytes as a sparse file.
 */
static void
write_wal_seg_with_size(const char *path, uint32_t tli, uint64_t pageaddr,
						uint32_t seg_size)
{
	FILE    *f;
	uint8_t  hdr[40] = {0};

	/* xlp_magic = 0xD071 (LE) */
	hdr[0] = 0x71; hdr[1] = 0xD0;
	/* xlp_info = XLP_LONG_HEADER (LE) */
	hdr[2] = 0x02; hdr[3] = 0x00;
	/* xlp_tli (LE) */
	hdr[4] = (uint8_t)(tli);
	hdr[5] = (uint8_t)(tli >> 8);
	hdr[6] = (uint8_t)(tli >> 16);
	hdr[7] = (uint8_t)(tli >> 24);
	/* xlp_pageaddr (LE) */
	hdr[8]  = (uint8_t)(pageaddr);
	hdr[9]  = (uint8_t)(pageaddr >> 8);
	hdr[10] = (uint8_t)(pageaddr >> 16);
	hdr[11] = (uint8_t)(pageaddr >> 24);
	hdr[12] = (uint8_t)(pageaddr >> 32);
	hdr[13] = (uint8_t)(pageaddr >> 40);
	hdr[14] = (uint8_t)(pageaddr >> 48);
	hdr[15] = (uint8_t)(pageaddr >> 56);
	/* xlp_sysid = 1 */
	hdr[24] = 0x01;
	/* xlp_seg_size (LE) */
	hdr[32] = (uint8_t)(seg_size);
	hdr[33] = (uint8_t)(seg_size >> 8);
	hdr[34] = (uint8_t)(seg_size >> 16);
	hdr[35] = (uint8_t)(seg_size >> 24);
	/* xlp_xlog_blcksz = 8192 (LE) */
	hdr[36] = 0x00; hdr[37] = 0x20; hdr[38] = 0x00; hdr[39] = 0x00;

	f = fopen(path, "wb");
	if (f == NULL)
		return;
	fwrite(hdr, 1, sizeof(hdr), f);

	/* Extend to full seg_size as a sparse file */
	(void)fseek(f, (long)seg_size - 1, SEEK_SET);
	(void)fputc('\0', f);

	fclose(f);
}

/*
 * With 1 MB segments: segments 1, 2, 3 all present.
 * backup.start_lsn = 0x100000 (seg 1), stop_lsn = 0x300000 (seg 3).
 * detect_wal_segment_size() should read 0x100000 from seg 1's header.
 * Expected: 0 errors from check_wal_availability().
 */
START_TEST(test_seg_size_1mb_avail)
{
	char            dir[PATH_MAX];
	char            seg[PATH_MAX];
	WALArchiveInfo *wi;
	BackupInfo      bi;
	ValidationResult *r;
	uint32_t        sz = 0x100000;   /* 1 MB */

	snprintf(dir, sizeof(dir), "/tmp/pg_sz1mb_%d", (int)getpid());
	mkdir(dir, 0755);

	for (int s = 1; s <= 3; s++)
	{
		snprintf(seg, sizeof(seg), "%s/0000000100000000%08X", dir, s);
		write_wal_seg_with_size(seg, 1, (uint64_t)s * sz, sz);
	}

	wi = scan_wal_archive(dir);
	ck_assert_ptr_nonnull(wi);
	ck_assert_int_eq(wi->segment_count, 3);

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "seg-size-1mb");
	bi.timeline  = 1;
	bi.start_lsn = 1 * (uint64_t)sz;   /* 0x100000 */
	bi.stop_lsn  = 3 * (uint64_t)sz;   /* 0x300000 */

	r = check_wal_availability(&bi, wi);
	free_wal_archive_info(wi);

	char cmd[PATH_MAX + 8];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
}
END_TEST

/*
 * With 1 MB segments: check_wal_headers() must compute expected_pageaddr
 * using the detected 1 MB size, not the hardcoded 16 MB default.
 * Same 3 segments; expected: 0 errors.
 */
START_TEST(test_seg_size_1mb_headers)
{
	char            dir[PATH_MAX];
	char            seg[PATH_MAX];
	WALArchiveInfo *wi;
	BackupInfo      bi;
	ValidationResult *r;
	uint32_t        sz = 0x100000;   /* 1 MB */

	snprintf(dir, sizeof(dir), "/tmp/pg_sz1mbh_%d", (int)getpid());
	mkdir(dir, 0755);

	for (int s = 1; s <= 3; s++)
	{
		snprintf(seg, sizeof(seg), "%s/0000000100000000%08X", dir, s);
		write_wal_seg_with_size(seg, 1, (uint64_t)s * sz, sz);
	}

	wi = scan_wal_archive(dir);
	ck_assert_ptr_nonnull(wi);

	memset(&bi, 0, sizeof(bi));
	strcpy(bi.backup_id, "seg-hdr-1mb");
	bi.timeline  = 1;
	bi.start_lsn = 1 * (uint64_t)sz;
	bi.stop_lsn  = 3 * (uint64_t)sz;

	r = check_wal_headers(&bi, wi);
	free_wal_archive_info(wi);

	char cmd[PATH_MAX + 8];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
	(void)system(cmd);

	ck_assert_ptr_nonnull(r);
	ck_assert_int_eq(r->error_count, 0);
	free_validation_result(r);
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
	TCase *tc_redo_lsn;
	TCase *tc_stream_wal;
	TCase *tc_seg_size;
	TCase *tc_rec_crc;
	TCase *tc_arch_headers;
	TCase *tc_restore_chain;
	TCase *tc_xchain;
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

	/* redo_lsn WAL range extension unit tests */
	tc_redo_lsn = tcase_create("redo_lsn");
	tcase_add_test(tc_redo_lsn, test_wal_avail_redo_same_segment);
	tcase_add_test(tc_redo_lsn, test_wal_avail_redo_earlier_present);
	tcase_add_test(tc_redo_lsn, test_wal_avail_redo_earlier_missing);
	suite_add_tcase(s, tc_redo_lsn);

	/* Stream backup WAL validation unit tests */
	tc_stream_wal = tcase_create("stream_wal");
	tcase_add_test(tc_stream_wal, test_stream_wal_all_present);
	tcase_add_test(tc_stream_wal, test_stream_wal_missing_segment);
	tcase_add_test(tc_stream_wal, test_stream_wal_empty_dir);
	suite_add_tcase(s, tc_stream_wal);

	/* Non-standard WAL segment size unit tests (BUG-002 fix) */
	tc_seg_size = tcase_create("seg_size");
	tcase_add_test(tc_seg_size, test_seg_size_1mb_avail);
	tcase_add_test(tc_seg_size, test_seg_size_1mb_headers);
	suite_add_tcase(s, tc_seg_size);

	/* Per-record CRC unit tests */
	tc_rec_crc = tcase_create("record_crc");
	tcase_add_test(tc_rec_crc, test_rec_crc_all_valid);
	tcase_add_test(tc_rec_crc, test_rec_crc_mismatch);
	tcase_add_test(tc_rec_crc, test_rec_crc_invalid_totlen);
	tcase_add_test(tc_rec_crc, test_rec_crc_zero_skipped);
	tcase_add_test(tc_rec_crc, test_rec_crc_multipage_skipped);
	tcase_add_test(tc_rec_crc, test_rec_crc_payload_valid);
	tcase_add_test(tc_rec_crc, test_rec_crc_payload_mismatch);
	suite_add_tcase(s, tc_rec_crc);

	/* Archive-wide header validation unit tests (BUG-003 fix) */
	tc_arch_headers = tcase_create("archive_headers");
	tcase_add_test(tc_arch_headers, test_archive_headers_null);
	tcase_add_test(tc_arch_headers, test_archive_headers_empty);
	tcase_add_test(tc_arch_headers, test_archive_headers_all_valid);
	tcase_add_test(tc_arch_headers, test_archive_headers_bad_pageaddr);
	tcase_add_test(tc_arch_headers, test_archive_headers_truncated);
	suite_add_tcase(s, tc_arch_headers);

	/* Restore-chain WAL continuity unit tests */
	tc_restore_chain = tcase_create("restore_chain");
	tcase_add_test(tc_restore_chain, test_chain_null_backups);
	tcase_add_test(tc_restore_chain, test_chain_null_archive);
	tcase_add_test(tc_restore_chain, test_chain_single_backup);
	tcase_add_test(tc_restore_chain, test_chain_adjacent_backups);
	tcase_add_test(tc_restore_chain, test_chain_orphan_skipped);
	tcase_add_test(tc_restore_chain, test_chain_complete_bridge);
	tcase_add_test(tc_restore_chain, test_chain_missing_bridge);
	tcase_add_test(tc_restore_chain, test_chain_different_timelines);
	suite_add_tcase(s, tc_restore_chain);

	/* Cross-timeline WAL chain validation */
	tc_xchain = tcase_create("cross_timeline_chain");
	tcase_add_test(tc_xchain, test_xchain_no_history_file);
	tcase_add_test(tc_xchain, test_xchain_pre_switch_complete);
	tcase_add_test(tc_xchain, test_xchain_pre_switch_missing);
	tcase_add_test(tc_xchain, test_xchain_post_switch_missing);
	tcase_add_test(tc_xchain, test_xchain_adjacent_to_switch);
	suite_add_tcase(s, tc_xchain);

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

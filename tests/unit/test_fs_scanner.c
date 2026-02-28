/*
 * test_fs_scanner.c
 *
 * Unit tests for fs_scanner: scan_backup_directory, scan_wal_archive
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
#include "test_integration_helpers.h"

/* ------------------------------------------------------------------ *
 * scan_backup_directory() — unit tests (no real backup needed)
 * ------------------------------------------------------------------ */

/*
 * Scanning a non-existent path returns NULL (no crash).
 */
START_TEST(test_scan_nonexistent_dir)
{
	BackupInfo *result = scan_backup_directory("/tmp/pg_fs_test_nonexistent_99999", -1);
	ck_assert_ptr_null(result);
}
END_TEST

/*
 * Scanning an empty directory returns NULL (no backups found).
 */
START_TEST(test_scan_empty_dir)
{
	char tmp_dir[64];
	snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/pg_fs_empty_%d", (int)getpid());
	mkdir(tmp_dir, 0755);

	BackupInfo *result = scan_backup_directory(tmp_dir, -1);
	ck_assert_ptr_null(result);

	rmdir(tmp_dir);
}
END_TEST

/*
 * free_backup_list(NULL) must not crash.
 */
START_TEST(test_free_null_list)
{
	free_backup_list(NULL);  /* must not crash */
}
END_TEST

/* ------------------------------------------------------------------ *
 * Integration tests: real pg_probackup catalog
 * Skipped if catalog not present.
 * ------------------------------------------------------------------ */

/*
 * scan_backup_directory() on a real pg_probackup catalog finds >= 1 backup.
 */
START_TEST(test_scan_real_catalog)
{
	INTEGRATION_SKIP();

	char backup_dir[PATH_MAX];
	snprintf(backup_dir, sizeof(backup_dir),
			 "%s/backups/%s", get_test_catalog(), get_test_instance());

	if (!is_directory(backup_dir))
	{
		fprintf(stderr, "SKIP: real catalog not found at %s\n", backup_dir);
		return;
	}

	BackupInfo *list = scan_backup_directory(backup_dir, -1);
	ck_assert_ptr_nonnull(list);

	/* Count backups */
	int count = 0;
	for (BackupInfo *cur = list; cur != NULL; cur = cur->next)
		count++;
	ck_assert_int_ge(count, 1);

	/* Each entry must have a non-empty backup_id and backup_path */
	for (BackupInfo *cur = list; cur != NULL; cur = cur->next)
	{
		ck_assert(cur->backup_id[0] != '\0');
		ck_assert(cur->backup_path[0] != '\0');
	}

	free_backup_list(list);
}
END_TEST

/*
 * scan_backup_directory() with max_depth=0 on a single backup directory
 * returns exactly that backup.
 */
START_TEST(test_scan_depth_zero)
{
	INTEGRATION_SKIP_BACKUP();

	char backup_path[PATH_MAX];
	snprintf(backup_path, sizeof(backup_path),
			 "%s/backups/%s/%s",
			 get_test_catalog(), get_test_instance(), get_test_backup_id());

	if (!is_directory(backup_path))
	{
		fprintf(stderr, "SKIP: real backup not found at %s\n", backup_path);
		return;
	}

	BackupInfo *list = scan_backup_directory(backup_path, 0);
	ck_assert_ptr_nonnull(list);
	ck_assert_str_eq(list->backup_id, get_test_backup_id());
	ck_assert_ptr_null(list->next);  /* depth=0 → only one entry */

	free_backup_list(list);
}
END_TEST

/* ------------------------------------------------------------------ *
 * scan_wal_archive() tests
 * ------------------------------------------------------------------ */

/*
 * scan_wal_archive() on a non-existent path returns NULL.
 */
START_TEST(test_scan_wal_nonexistent)
{
	WALArchiveInfo *info = scan_wal_archive("/tmp/pg_wal_nonexistent_99999");
	ck_assert_ptr_null(info);
}
END_TEST

/*
 * scan_wal_archive() on the real archive finds expected segments.
 */
START_TEST(test_scan_wal_real)
{
	INTEGRATION_SKIP();

	char wal_path[PATH_MAX];
	snprintf(wal_path, sizeof(wal_path),
			 "%s/wal/%s", get_test_catalog(), get_test_instance());

	if (!is_directory(wal_path))
	{
		fprintf(stderr, "SKIP: real WAL archive not found at %s\n", wal_path);
		return;
	}

	WALArchiveInfo *info = scan_wal_archive(wal_path);
	ck_assert_ptr_nonnull(info);
	ck_assert_int_ge(info->segment_count, 9);  /* we know >= 9 segments exist */

	/* All segments must be on timeline 1 */
	for (int i = 0; i < info->segment_count; i++)
		ck_assert_int_eq(info->segments[i].timeline, 1);

	free_wal_archive_info(info);
}
END_TEST

/* ------------------------------------------------------------------ *
 * Suite assembly
 * ------------------------------------------------------------------ */

Suite*
fs_scanner_suite(void)
{
	Suite *s = suite_create("fs_scanner");

	TCase *tc_unit = tcase_create("Unit");
	tcase_add_test(tc_unit, test_scan_nonexistent_dir);
	tcase_add_test(tc_unit, test_scan_empty_dir);
	tcase_add_test(tc_unit, test_free_null_list);
	suite_add_tcase(s, tc_unit);

	TCase *tc_int = tcase_create("Integration");
	tcase_add_test(tc_int, test_scan_real_catalog);
	tcase_add_test(tc_int, test_scan_depth_zero);
	tcase_add_test(tc_int, test_scan_wal_nonexistent);
	tcase_add_test(tc_int, test_scan_wal_real);
	suite_add_tcase(s, tc_int);

	return s;
}

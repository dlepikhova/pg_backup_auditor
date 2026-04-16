/*
 * test_file_utils.c
 *
 * Unit tests for file utility functions
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
static char test_file[PATH_MAX];
static char test_subdir[PATH_MAX];

/* Setup test directories and files */
static void
setup_test_files(void)
{
	snprintf(test_dir, sizeof(test_dir), "/tmp/file_utils_test_%d", getpid());
	mkdir(test_dir, 0755);

	snprintf(test_file, sizeof(test_file), "%s/testfile.txt", test_dir);
	FILE *fp = fopen(test_file, "w");
	fprintf(fp, "hello world\n");
	fclose(fp);

	snprintf(test_subdir, sizeof(test_subdir), "%s/subdir", test_dir);
	mkdir(test_subdir, 0755);

	/* Create some files in subdir */
	char subfile[PATH_MAX];
	snprintf(subfile, sizeof(subfile), "%s/file1.txt", test_subdir);
	fp = fopen(subfile, "w");
	fprintf(fp, "content1\n");
	fclose(fp);

	snprintf(subfile, sizeof(subfile), "%s/file2.txt", test_subdir);
	fp = fopen(subfile, "w");
	fprintf(fp, "content2\n");
	fclose(fp);
}

/* Teardown test directories */
static void
teardown_test_files(void)
{
	char cmd[PATH_MAX + 20];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
	system(cmd);
}

/* Test: file_exists for existing file */
START_TEST(test_file_exists_true)
{
	setup_test_files();

	bool result = file_exists(test_file);
	ck_assert_int_eq(result, true);

	teardown_test_files();
}
END_TEST

/* Test: file_exists for non-existent file */
START_TEST(test_file_exists_false)
{
	setup_test_files();

	char nonexistent[PATH_MAX];
	snprintf(nonexistent, sizeof(nonexistent), "%s/nonexistent.txt", test_dir);

	bool result = file_exists(nonexistent);
	ck_assert_int_eq(result, false);

	teardown_test_files();
}
END_TEST

/* Test: is_directory for directory */
START_TEST(test_is_directory_true)
{
	setup_test_files();

	bool result = is_directory(test_subdir);
	ck_assert_int_eq(result, true);

	teardown_test_files();
}
END_TEST

/* Test: is_directory for file (not directory) */
START_TEST(test_is_directory_false)
{
	setup_test_files();

	bool result = is_directory(test_file);
	ck_assert_int_eq(result, false);

	teardown_test_files();
}
END_TEST

/* Test: is_regular_file for file */
START_TEST(test_is_regular_file_true)
{
	setup_test_files();

	bool result = is_regular_file(test_file);
	ck_assert_int_eq(result, true);

	teardown_test_files();
}
END_TEST

/* Test: is_regular_file for directory (not file) */
START_TEST(test_is_regular_file_false)
{
	setup_test_files();

	bool result = is_regular_file(test_subdir);
	ck_assert_int_eq(result, false);

	teardown_test_files();
}
END_TEST

/* Test: get_file_size */
START_TEST(test_get_file_size)
{
	setup_test_files();

	off_t size = get_file_size(test_file);
	/* "hello world\n" = 12 bytes */
	ck_assert_int_eq(size, 12);

	teardown_test_files();
}
END_TEST

/* Test: get_directory_size (sum of all files in directory tree) */
START_TEST(test_get_directory_size)
{
	setup_test_files();

	uint64_t size = get_directory_size(test_subdir);
	/* file1.txt: "content1\n" = 9 bytes
	 * file2.txt: "content2\n" = 9 bytes
	 * Total = 18 bytes (at minimum; may include directory metadata) */
	ck_assert_int_ge(size, 18);

	teardown_test_files();
}
END_TEST

/* Test: path_join with normal paths */
START_TEST(test_path_join_normal)
{
	char result[PATH_MAX];
	path_join(result, sizeof(result), "/home/user", "backup");

	ck_assert_str_eq(result, "/home/user/backup");
}
END_TEST

/* Test: path_join with trailing slash */
START_TEST(test_path_join_trailing_slash)
{
	char result[PATH_MAX];
	path_join(result, sizeof(result), "/home/user/", "backup");

	ck_assert_str_eq(result, "/home/user/backup");
}
END_TEST

/* Test: path_join with empty first path */
START_TEST(test_path_join_empty_first)
{
	char result[PATH_MAX];
	path_join(result, sizeof(result), "", "backup");

	ck_assert_str_eq(result, "backup");
}
END_TEST

/* Test: compute_file_crc32c */
START_TEST(test_compute_file_crc32c)
{
	setup_test_files();

	uint32_t crc = 0;
	bool success = compute_file_crc32c(test_file, &crc);

	ck_assert_int_eq(success, true);
	/* CRC should be non-zero for non-empty file */
	ck_assert_uint_ne(crc, 0U);

	teardown_test_files();
}
END_TEST

/* Test: compute_file_crc32c for non-existent file */
START_TEST(test_compute_file_crc32c_nonexistent)
{
	setup_test_files();

	char nonexistent[PATH_MAX];
	snprintf(nonexistent, sizeof(nonexistent), "%s/nonexistent.txt", test_dir);

	uint32_t crc = 0;
	bool success = compute_file_crc32c(nonexistent, &crc);

	ck_assert_int_eq(success, false);

	teardown_test_files();
}
END_TEST

/* Create test suite */
Suite *
file_utils_suite(void)
{
	Suite *s;
	TCase *tc_file_ops;
	TCase *tc_path;
	TCase *tc_crc;

	s = suite_create("File Utils");

	tc_file_ops = tcase_create("file_operations");
	tcase_add_test(tc_file_ops, test_file_exists_true);
	tcase_add_test(tc_file_ops, test_file_exists_false);
	tcase_add_test(tc_file_ops, test_is_directory_true);
	tcase_add_test(tc_file_ops, test_is_directory_false);
	tcase_add_test(tc_file_ops, test_is_regular_file_true);
	tcase_add_test(tc_file_ops, test_is_regular_file_false);
	tcase_add_test(tc_file_ops, test_get_file_size);
	tcase_add_test(tc_file_ops, test_get_directory_size);
	suite_add_tcase(s, tc_file_ops);

	tc_path = tcase_create("path_operations");
	tcase_add_test(tc_path, test_path_join_normal);
	tcase_add_test(tc_path, test_path_join_trailing_slash);
	tcase_add_test(tc_path, test_path_join_empty_first);
	suite_add_tcase(s, tc_path);

	tc_crc = tcase_create("crc32c");
	tcase_add_test(tc_crc, test_compute_file_crc32c);
	tcase_add_test(tc_crc, test_compute_file_crc32c_nonexistent);
	suite_add_tcase(s, tc_crc);

	return s;
}

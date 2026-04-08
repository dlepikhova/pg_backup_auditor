/*
 * test_pg_probackup.c
 *
 * Unit tests for pg_probackup adapter:
 *   pg_probackup_detect(), pg_probackup_scan(), backup.control parsing
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

/* ------------------------------------------------------------------ *
 * Helpers
 * ------------------------------------------------------------------ */

/*
 * Create a minimal pg_probackup backup directory in a temp location.
 * Returns the path (must be rmdir'd by caller).
 */
static void
make_probackup_dir(const char *base, bool with_control, bool with_database)
{
	mkdir(base, 0755);

	if (with_control)
	{
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "%s/backup.control", base);
		FILE *f = fopen(path, "w");
		if (f) { fputs("backup-id = TESTXXX\n", f); fclose(f); }
	}

	if (with_database)
	{
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "%s/database", base);
		mkdir(path, 0755);
	}
}

static void
rm_rf(const char *path)
{
	char cmd[PATH_MAX + 8];
	snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
	(void)system(cmd);
}

/* ------------------------------------------------------------------ *
 * detect() tests
 * ------------------------------------------------------------------ */

/* Non-existent path → false */
START_TEST(test_detect_nonexistent)
{
	ck_assert(!pg_probackup_adapter.detect("/tmp/pg_pp_nonexistent_99999"));
}
END_TEST

/* Directory with both backup.control and database/ → true */
START_TEST(test_detect_valid)
{
	char tmp[64];
	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_valid_%d", (int)getpid());
	make_probackup_dir(tmp, true, true);

	ck_assert(pg_probackup_adapter.detect(tmp));

	rm_rf(tmp);
}
END_TEST

/* Directory missing backup.control → false */
START_TEST(test_detect_no_control)
{
	char tmp[64];
	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_noctl_%d", (int)getpid());
	make_probackup_dir(tmp, false, true);

	ck_assert(!pg_probackup_adapter.detect(tmp));

	rm_rf(tmp);
}
END_TEST

/* Directory missing database/ → false */
START_TEST(test_detect_no_database)
{
	char tmp[64];
	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_nodb_%d", (int)getpid());
	make_probackup_dir(tmp, true, false);

	ck_assert(!pg_probackup_adapter.detect(tmp));

	rm_rf(tmp);
}
END_TEST

/* ------------------------------------------------------------------ *
 * backup.control parsing — additional backup-mode / status branches
 * ------------------------------------------------------------------ */

/*
 * Helper: write a backup.control with given mode and status,
 * scan the backup, return BackupInfo*.
 * Caller must free_backup_list() the result.
 */
static BackupInfo*
scan_with_control(const char *tmp_base, const char *mode, const char *status)
{
	char db_dir[PATH_MAX];
	char ctl_path[PATH_MAX];

	snprintf(db_dir,  sizeof(db_dir),  "%s/database", tmp_base);
	snprintf(ctl_path, sizeof(ctl_path), "%s/backup.control", tmp_base);

	mkdir(tmp_base, 0755);
	mkdir(db_dir,   0755);

	FILE *f = fopen(ctl_path, "w");
	if (!f) return NULL;
	fprintf(f, "backup-id = TSTABC\n");
	fprintf(f, "backup-mode = %s\n", mode);
	fprintf(f, "status = %s\n", status);
	fprintf(f, "start-lsn = 0/1000028\n");
	fprintf(f, "stop-lsn = 0/2000028\n");
	fclose(f);

	return pg_probackup_adapter.scan(tmp_base);
}

/* RUNNING status is parsed correctly */
START_TEST(test_parse_status_running)
{
	char tmp[64];
	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_run_%d", (int)getpid());

	BackupInfo *b = scan_with_control(tmp, "FULL", "RUNNING");
	rm_rf(tmp);

	ck_assert_ptr_nonnull(b);
	ck_assert_int_eq(b->status, BACKUP_STATUS_RUNNING);
	free_backup_list(b);
}
END_TEST

/* PAGE backup-mode is parsed correctly */
START_TEST(test_parse_mode_page)
{
	char tmp[64];
	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_page_%d", (int)getpid());

	BackupInfo *b = scan_with_control(tmp, "PAGE", "OK");
	rm_rf(tmp);

	ck_assert_ptr_nonnull(b);
	ck_assert_int_eq(b->type, BACKUP_TYPE_PAGE);
	free_backup_list(b);
}
END_TEST

/* PTRACK backup-mode is parsed correctly */
START_TEST(test_parse_mode_ptrack)
{
	char tmp[64];
	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_ptrack_%d", (int)getpid());

	BackupInfo *b = scan_with_control(tmp, "PTRACK", "OK");
	rm_rf(tmp);

	ck_assert_ptr_nonnull(b);
	ck_assert_int_eq(b->type, BACKUP_TYPE_PTRACK);
	free_backup_list(b);
}
END_TEST

/* ORPHAN status is parsed correctly */
START_TEST(test_parse_status_orphan)
{
	char tmp[64];
	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_orphan_%d", (int)getpid());

	BackupInfo *b = scan_with_control(tmp, "DELTA", "ORPHAN");
	rm_rf(tmp);

	ck_assert_ptr_nonnull(b);
	ck_assert_int_eq(b->status, BACKUP_STATUS_ORPHAN);
	free_backup_list(b);
}
END_TEST

/* from-replica=true → backup_from="standby" */
START_TEST(test_parse_from_replica_standby)
{
	char tmp[64];
	char db_dir[PATH_MAX];
	char ctl_path[PATH_MAX];

	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_replica_%d", (int)getpid());
	snprintf(db_dir,  sizeof(db_dir),  "%s/database", tmp);
	snprintf(ctl_path, sizeof(ctl_path), "%s/backup.control", tmp);

	mkdir(tmp, 0755);
	mkdir(db_dir, 0755);

	FILE *f = fopen(ctl_path, "w");
	ck_assert_ptr_nonnull(f);
	fprintf(f, "backup-id = TSTDEF\n");
	fprintf(f, "backup-mode = FULL\n");
	fprintf(f, "status = OK\n");
	fprintf(f, "from-replica = true\n");
	fprintf(f, "compress-alg = zlib\n");
	fclose(f);

	BackupInfo *b = pg_probackup_adapter.scan(tmp);
	rm_rf(tmp);

	ck_assert_ptr_nonnull(b);
	ck_assert_str_eq(b->backup_from, "standby");
	ck_assert_str_eq(b->compress_alg, "zlib");
	free_backup_list(b);
}
END_TEST

/* from-replica=false → backup_from="primary"; compress-alg=none → compress_alg="" */
START_TEST(test_parse_from_replica_primary)
{
	char tmp[64];
	char db_dir[PATH_MAX];
	char ctl_path[PATH_MAX];

	snprintf(tmp, sizeof(tmp), "/tmp/pg_pp_primary_%d", (int)getpid());
	snprintf(db_dir,  sizeof(db_dir),  "%s/database", tmp);
	snprintf(ctl_path, sizeof(ctl_path), "%s/backup.control", tmp);

	mkdir(tmp, 0755);
	mkdir(db_dir, 0755);

	FILE *f = fopen(ctl_path, "w");
	ck_assert_ptr_nonnull(f);
	fprintf(f, "backup-id = TSTGHI\n");
	fprintf(f, "backup-mode = FULL\n");
	fprintf(f, "status = OK\n");
	fprintf(f, "from-replica = false\n");
	fprintf(f, "compress-alg = none\n");
	fclose(f);

	BackupInfo *b = pg_probackup_adapter.scan(tmp);
	rm_rf(tmp);

	ck_assert_ptr_nonnull(b);
	ck_assert_str_eq(b->backup_from, "primary");
	ck_assert_str_eq(b->compress_alg, "");  /* "none" is suppressed */
	free_backup_list(b);
}
END_TEST

/* ------------------------------------------------------------------ *
 * Integration: real backup
 * ------------------------------------------------------------------ */

/* scan() on a real backup returns correct metadata */
START_TEST(test_scan_real_full_backup)
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

	BackupInfo *b = pg_probackup_adapter.scan(backup_path);
	ck_assert_ptr_nonnull(b);
	ck_assert_str_eq(b->backup_id, get_test_backup_id());
	ck_assert_int_eq(b->type,   BACKUP_TYPE_FULL);
	ck_assert_int_eq(b->status, BACKUP_STATUS_OK);
	ck_assert_int_eq(b->wal_stream, 0);  /* archive mode */
	ck_assert(b->start_lsn > 0);
	ck_assert(b->stop_lsn  > 0);

	free_backup_list(b);
}
END_TEST

/* ------------------------------------------------------------------ *
 * Suite assembly
 * ------------------------------------------------------------------ */

Suite*
pg_probackup_suite(void)
{
	Suite *s = suite_create("pg_probackup Adapter");

	TCase *tc_detect = tcase_create("detect");
	tcase_add_test(tc_detect, test_detect_nonexistent);
	tcase_add_test(tc_detect, test_detect_valid);
	tcase_add_test(tc_detect, test_detect_no_control);
	tcase_add_test(tc_detect, test_detect_no_database);
	suite_add_tcase(s, tc_detect);

	TCase *tc_parse = tcase_create("parse_backup_control");
	tcase_add_test(tc_parse, test_parse_status_running);
	tcase_add_test(tc_parse, test_parse_mode_page);
	tcase_add_test(tc_parse, test_parse_mode_ptrack);
	tcase_add_test(tc_parse, test_parse_status_orphan);
	tcase_add_test(tc_parse, test_parse_from_replica_standby);
	tcase_add_test(tc_parse, test_parse_from_replica_primary);
	suite_add_tcase(s, tc_parse);

	TCase *tc_int = tcase_create("Integration");
	tcase_add_test(tc_int, test_scan_real_full_backup);
	suite_add_tcase(s, tc_int);

	return s;
}

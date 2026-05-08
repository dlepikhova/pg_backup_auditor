/*
 * test_anomaly_detection.c
 *
 * Unit tests for anomaly detection in audit command
 *
 * Copyright (C) 2026 Daria Lepikhova
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <check.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdbool.h>
#include "../include/types.h"

/* Forward declarations of functions we're testing */
typedef struct {
	BackupTool tool;
	BackupType type;
	int count;
	uint64_t total_size;
	time_t total_duration;
	double avg_size;
	double avg_duration;
	double stddev_size;
} BackupTypeStats;

typedef struct {
	char backup_id[64];
	const char *anomaly_type;
	double value;
	double avg;
	double multiplier;
} AnomalyRecord;

typedef struct {
	AnomalyRecord *items;
	int count;
	int capacity;
} AnomalyList;

BackupTypeStats* calculate_backup_stats(BackupInfo *backups, int *out_count);
AnomalyList* detect_anomalies(BackupInfo *backups, BackupTypeStats *stats, int stats_count, bool detect_size_small);

/* Helper to create a backup */
static BackupInfo*
create_backup(const char *id, BackupTool tool, BackupType type,
			  uint64_t size, time_t start, time_t duration)
{
	BackupInfo *b = calloc(1, sizeof(BackupInfo));
	strncpy(b->backup_id, id, sizeof(b->backup_id) - 1);
	b->tool = tool;
	b->type = type;
	b->status = BACKUP_STATUS_OK;
	b->data_bytes = size;
	b->wal_bytes = 0;
	b->start_time = start;
	b->end_time = start + duration;
	return b;
}

/* Test: statistics per tool+type (not mixed) */
START_TEST(test_stats_per_tool_type)
{
	time_t now = time(NULL);
	BackupInfo head;
	memset(&head, 0, sizeof(head));

	/* Create test backups */
	BackupInfo *b1 = create_backup("pg_base_full_1", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1000000000, now, 3600);
	BackupInfo *b2 = create_backup("pg_base_full_2", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1100000000, now + 86400, 3600);
	BackupInfo *b3 = create_backup("pg_pro_full_1", BACKUP_TOOL_PG_PROBACKUP, BACKUP_TYPE_FULL, 500000000, now, 1800);
	BackupInfo *b4 = create_backup("pg_pro_full_2", BACKUP_TOOL_PG_PROBACKUP, BACKUP_TYPE_FULL, 550000000, now + 86400, 1800);

	/* Link them */
	b1->next = b2;
	b2->next = b3;
	b3->next = b4;
	b4->next = NULL;

	int count = 0;
	BackupTypeStats *stats = calculate_backup_stats(b1, &count);

	ck_assert_ptr_nonnull(stats);
	ck_assert_int_eq(count, 2);  /* Two combinations: (pg_basebackup, FULL) and (pg_probackup, FULL) */

	/* Find each stat and verify it's separate */
	BackupTypeStats *st_pgbase = NULL, *st_pgpro = NULL;
	for (int i = 0; i < count; i++)
	{
		if (stats[i].tool == BACKUP_TOOL_PG_BASEBACKUP && stats[i].type == BACKUP_TYPE_FULL)
			st_pgbase = &stats[i];
		if (stats[i].tool == BACKUP_TOOL_PG_PROBACKUP && stats[i].type == BACKUP_TYPE_FULL)
			st_pgpro = &stats[i];
	}

	ck_assert_ptr_nonnull(st_pgbase);
	ck_assert_ptr_nonnull(st_pgpro);

	/* pg_basebackup avg should be ~1.05 GB */
	ck_assert_int_eq(st_pgbase->count, 2);
	ck_assert(st_pgbase->avg_size > 1000000000 && st_pgbase->avg_size < 1200000000);

	/* pg_probackup avg should be ~0.525 GB */
	ck_assert_int_eq(st_pgpro->count, 2);
	ck_assert(st_pgpro->avg_size > 500000000 && st_pgpro->avg_size < 600000000);

	/* Clean up */
	free(stats);
	free(b1);
	free(b2);
	free(b3);
	free(b4);
}
END_TEST

/* Test: anomaly detection finds oversized backups */
START_TEST(test_anomaly_large_size)
{
	time_t now = time(NULL);

	/* Create 5 normal + 1 anomaly to avoid skewing the average */
	BackupInfo *b1 = create_backup("b1", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1000000000, now, 3600);
	BackupInfo *b2 = create_backup("b2", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1100000000, now + 86400, 3600);
	BackupInfo *b3 = create_backup("b3", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1050000000, now + 172800, 3600);
	BackupInfo *b4 = create_backup("b4", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1020000000, now + 259200, 3600);
	BackupInfo *b5 = create_backup("b5", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 980000000, now + 345600, 3600);
	BackupInfo *b6 = create_backup("anomaly", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 5000000000, now + 432000, 3600);  /* ~4.5x average */

	b1->next = b2;
	b2->next = b3;
	b3->next = b4;
	b4->next = b5;
	b5->next = b6;
	b6->next = NULL;

	int count = 0;
	BackupTypeStats *stats = calculate_backup_stats(b1, &count);
	AnomalyList *anomalies = detect_anomalies(b1, stats, count, true);

	ck_assert_ptr_nonnull(anomalies);
	ck_assert_int_eq(anomalies->count, 1);  /* Should find 1 anomaly */
	ck_assert_str_eq(anomalies->items[0].backup_id, "anomaly");
	ck_assert_str_eq(anomalies->items[0].anomaly_type, "size_large");
	ck_assert(anomalies->items[0].multiplier > 2.5);

	free(anomalies->items);
	free(anomalies);
	free(stats);
	free(b1);
	free(b2);
	free(b3);
	free(b4);
	free(b5);
	free(b6);
}
END_TEST

/* Test: different tools don't interfere */
START_TEST(test_anomaly_not_mixed_tools)
{
	time_t now = time(NULL);

	/* pg_basebackup: avg 1GB */
	BackupInfo *b1 = create_backup("pgbase1", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1000000000, now, 3600);
	BackupInfo *b2 = create_backup("pgbase2", BACKUP_TOOL_PG_BASEBACKUP, BACKUP_TYPE_FULL, 1100000000, now + 86400, 3600);

	/* pg_probackup: avg 500MB - much smaller, but NOT anomaly for pg_probackup */
	BackupInfo *b3 = create_backup("pgpro1", BACKUP_TOOL_PG_PROBACKUP, BACKUP_TYPE_FULL, 500000000, now, 1800);
	BackupInfo *b4 = create_backup("pgpro2", BACKUP_TOOL_PG_PROBACKUP, BACKUP_TYPE_FULL, 550000000, now + 86400, 1800);

	b1->next = b2;
	b2->next = b3;
	b3->next = b4;
	b4->next = NULL;

	int count = 0;
	BackupTypeStats *stats = calculate_backup_stats(b1, &count);
	AnomalyList *anomalies = detect_anomalies(b1, stats, count, true);

	/* pg_probackup 500MB is NOT anomalous for pg_probackup (it's the average) */
	ck_assert_ptr_nonnull(anomalies);
	ck_assert_int_eq(anomalies->count, 0);  /* No anomalies - sizes are normal for each tool */

	free(anomalies->items);
	free(anomalies);
	free(stats);
	free(b1);
	free(b2);
	free(b3);
	free(b4);
}
END_TEST

Suite *
anomaly_detection_suite(void)
{
	Suite *s = suite_create("Anomaly Detection");
	TCase *tc_core = tcase_create("Core");

	tcase_add_test(tc_core, test_stats_per_tool_type);
	tcase_add_test(tc_core, test_anomaly_large_size);
	tcase_add_test(tc_core, test_anomaly_not_mixed_tools);

	suite_add_tcase(s, tc_core);
	return s;
}

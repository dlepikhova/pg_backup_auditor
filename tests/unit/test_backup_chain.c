/*
 * test_backup_chain.c
 *
 * Unit tests for chain grouping (src/common/backup_chain.c).
 *
 * Copyright (C) 2026 Daria Lepikhova
 *
 * Licensed under the GNU GPL v3.0 or later.
 */

#include <check.h>
#include <stdlib.h>
#include <string.h>

#include "pg_backup_auditor.h"
#include "backup_chain.h"

/*
 * Allocate a BackupInfo populated with just the fields chain grouping
 * cares about: backup_id, type, parent_backup_id, start_time. The node
 * is prepended to *head so callers can build a list in a few lines.
 *
 * The whole list is freed via free_chain_list() below — we cannot reuse
 * free_backup_list() because that helper closes file descriptors and
 * frees adapter-allocated buffers we did not set.
 */
static BackupInfo *
mk(BackupInfo **head, const char *id, BackupType type, const char *parent, time_t t)
{
	BackupInfo *b = calloc(1, sizeof(*b));
	strncpy(b->backup_id, id, sizeof(b->backup_id) - 1);
	if (parent != NULL)
		strncpy(b->parent_backup_id, parent, sizeof(b->parent_backup_id) - 1);
	b->type = type;
	b->start_time = t;
	b->next = *head;
	*head = b;
	return b;
}

static void
free_chain_list(BackupInfo *head)
{
	while (head != NULL)
	{
		BackupInfo *n = head->next;
		free(head);
		head = n;
	}
}

/* Empty list yields no chains. */
START_TEST(test_backup_chain_empty)
{
	int n = 42;
	BackupChain *c = backup_chain_build(NULL, &n);
	ck_assert_ptr_nonnull(c);
	ck_assert_int_eq(n, 0);
	backup_chain_free(c, n);
}
END_TEST

/* Single FULL → one chain of size 1. */
START_TEST(test_backup_chain_single_full)
{
	BackupInfo *head = NULL;
	mk(&head, "F1", BACKUP_TYPE_FULL, NULL, 1000);

	int n = 0;
	BackupChain *c = backup_chain_build(head, &n);
	ck_assert_int_eq(n, 1);
	ck_assert_int_eq(c[0].count, 1);
	ck_assert_str_eq(c[0].root->backup_id, "F1");

	backup_chain_free(c, n);
	free_chain_list(head);
}
END_TEST

/* FULL → DELTA → DELTA, all in one chain sorted oldest-first. */
START_TEST(test_backup_chain_full_with_incrementals)
{
	BackupInfo *head = NULL;
	mk(&head, "D2", BACKUP_TYPE_DELTA, "D1", 3000);
	mk(&head, "D1", BACKUP_TYPE_DELTA, "F1", 2000);
	mk(&head, "F1", BACKUP_TYPE_FULL,  NULL, 1000);

	int n = 0;
	BackupChain *c = backup_chain_build(head, &n);
	ck_assert_int_eq(n, 1);
	ck_assert_int_eq(c[0].count, 3);
	/* Sorted by start_time, oldest first */
	ck_assert_str_eq(c[0].members[0]->backup_id, "F1");
	ck_assert_str_eq(c[0].members[1]->backup_id, "D1");
	ck_assert_str_eq(c[0].members[2]->backup_id, "D2");

	backup_chain_free(c, n);
	free_chain_list(head);
}
END_TEST

/* Two independent FULLs → two chains. */
START_TEST(test_backup_chain_multiple_fulls)
{
	BackupInfo *head = NULL;
	mk(&head, "F2", BACKUP_TYPE_FULL, NULL, 5000);
	mk(&head, "F1", BACKUP_TYPE_FULL, NULL, 1000);

	int n = 0;
	BackupChain *c = backup_chain_build(head, &n);
	ck_assert_int_eq(n, 2);
	ck_assert_int_eq(c[0].count, 1);
	ck_assert_int_eq(c[1].count, 1);

	backup_chain_free(c, n);
	free_chain_list(head);
}
END_TEST

/* DELTA whose parent does not exist → goes to the orphan bucket. */
START_TEST(test_backup_chain_orphan)
{
	BackupInfo *head = NULL;
	mk(&head, "D1", BACKUP_TYPE_DELTA, "MISSING", 2000);
	mk(&head, "F1", BACKUP_TYPE_FULL,  NULL,      1000);

	int n = 0;
	BackupChain *c = backup_chain_build(head, &n);
	/* 1 chain for F1 + 1 orphan bucket */
	ck_assert_int_eq(n, 2);
	/* Last bucket is orphans (root == NULL) */
	ck_assert_ptr_null(c[1].root);
	ck_assert_int_eq(c[1].count, 1);
	ck_assert_str_eq(c[1].members[0]->backup_id, "D1");

	backup_chain_free(c, n);
	free_chain_list(head);
}
END_TEST

/* find_root walks parent links up to the FULL. */
START_TEST(test_backup_chain_find_root)
{
	BackupInfo *head = NULL;
	BackupInfo *d2 = mk(&head, "D2", BACKUP_TYPE_DELTA, "D1", 3000);
	mk(&head, "D1", BACKUP_TYPE_DELTA, "F1", 2000);
	BackupInfo *f1 = mk(&head, "F1", BACKUP_TYPE_FULL,  NULL, 1000);

	ck_assert_ptr_eq(backup_chain_find_root(d2, head), f1);
	ck_assert_ptr_eq(backup_chain_find_root(f1, head), f1);

	free_chain_list(head);
}
END_TEST

/* find_backup returns the matching node, or NULL when missing. */
START_TEST(test_backup_chain_find_backup)
{
	BackupInfo *head = NULL;
	BackupInfo *f1 = mk(&head, "F1", BACKUP_TYPE_FULL, NULL, 1000);

	ck_assert_ptr_eq(backup_chain_find_backup(head, "F1"), f1);
	ck_assert_ptr_null(backup_chain_find_backup(head, "NOPE"));

	free_chain_list(head);
}
END_TEST

Suite *
backup_chain_suite(void)
{
	Suite *s = suite_create("backup_chain");

	TCase *tc = tcase_create("build_and_find");
	tcase_add_test(tc, test_backup_chain_empty);
	tcase_add_test(tc, test_backup_chain_single_full);
	tcase_add_test(tc, test_backup_chain_full_with_incrementals);
	tcase_add_test(tc, test_backup_chain_multiple_fulls);
	tcase_add_test(tc, test_backup_chain_orphan);
	tcase_add_test(tc, test_backup_chain_find_root);
	tcase_add_test(tc, test_backup_chain_find_backup);
	suite_add_tcase(s, tc);

	return s;
}

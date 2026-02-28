/*
 * test_integration_helpers.h
 *
 * Shared helpers for integration tests that require a real backup catalog.
 * Tests are skipped when the required environment variables are not set.
 *
 * Required environment variables:
 *   PG_BACKUP_AUDITOR_TEST_CATALOG   — path to the backup catalog root
 *   PG_BACKUP_AUDITOR_TEST_INSTANCE  — instance name (subdirectory under
 *                                      backups/ and wal/)
 *   PG_BACKUP_AUDITOR_TEST_BACKUP_ID — specific backup ID for per-backup tests
 *
 * Example:
 *   export PG_BACKUP_AUDITOR_TEST_CATALOG=/path/to/backup
 *   export PG_BACKUP_AUDITOR_TEST_INSTANCE=myinstance
 *   export PG_BACKUP_AUDITOR_TEST_BACKUP_ID=TB50D5
 *   make run
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

#ifndef TEST_INTEGRATION_HELPERS_H
#define TEST_INTEGRATION_HELPERS_H

#include <stdlib.h>
#include <stdio.h>

static inline const char *
get_test_catalog(void)
{
	return getenv("PG_BACKUP_AUDITOR_TEST_CATALOG");
}

static inline const char *
get_test_instance(void)
{
	return getenv("PG_BACKUP_AUDITOR_TEST_INSTANCE");
}

static inline const char *
get_test_backup_id(void)
{
	return getenv("PG_BACKUP_AUDITOR_TEST_BACKUP_ID");
}

/*
 * Skip the current test if catalog + instance env vars are not set.
 * Use at the top of tests that scan the catalog or WAL archive.
 */
#define INTEGRATION_SKIP() \
	do { \
		if (!get_test_catalog() || !get_test_instance()) \
		{ \
			fprintf(stderr, \
					"SKIP: set PG_BACKUP_AUDITOR_TEST_CATALOG and " \
					"PG_BACKUP_AUDITOR_TEST_INSTANCE to run integration tests\n"); \
			return; \
		} \
	} while (0)

/*
 * Skip the current test if catalog + instance + backup_id env vars are not set.
 * Use at the top of tests that operate on a specific backup.
 */
#define INTEGRATION_SKIP_BACKUP() \
	do { \
		if (!get_test_catalog() || !get_test_instance() || !get_test_backup_id()) \
		{ \
			fprintf(stderr, \
					"SKIP: set PG_BACKUP_AUDITOR_TEST_CATALOG, " \
					"PG_BACKUP_AUDITOR_TEST_INSTANCE, and " \
					"PG_BACKUP_AUDITOR_TEST_BACKUP_ID to run per-backup " \
					"integration tests\n"); \
			return; \
		} \
	} while (0)

#endif /* TEST_INTEGRATION_HELPERS_H */

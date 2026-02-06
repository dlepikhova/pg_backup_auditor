/*
 * test_runner.c
 *
 * Main test runner for unit tests
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
#include <stdlib.h>

/* External suite declarations */
extern Suite *string_utils_suite(void);
extern Suite *adapter_registry_suite(void);
extern Suite *pg_basebackup_suite(void);
extern Suite *sorting_suite(void);
extern Suite *xlog_suite(void);

int
main(void)
{
	int number_failed;
	SRunner *sr;

	/* Create suite runner */
	sr = srunner_create(string_utils_suite());

	/* Add additional suites */
	srunner_add_suite(sr, adapter_registry_suite());
	srunner_add_suite(sr, pg_basebackup_suite());
	srunner_add_suite(sr, sorting_suite());
	srunner_add_suite(sr, xlog_suite());

	/* Run tests */
	srunner_run_all(sr, CK_NORMAL);

	/* Get number of failures */
	number_failed = srunner_ntests_failed(sr);

	/* Cleanup */
	srunner_free(sr);

	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

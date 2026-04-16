/*
 * test_crc32c.c
 *
 * Unit tests for CRC32C module
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
#include <stdint.h>
#include <string.h>
#include "crc32c.h"

/* Test: CRC32C of empty buffer */
START_TEST(test_crc32c_empty)
{
	uint32_t crc = ~0U;
	crc = crc32c_update(crc, NULL, 0);
	uint32_t result = ~crc;
	/* CRC of empty data should be 0 */
	ck_assert_uint_eq(result, 0U);
}
END_TEST

/* Test: CRC32C of simple data */
START_TEST(test_crc32c_simple)
{
	const uint8_t data[] = "hello";
	uint32_t crc = ~0U;
	crc = crc32c_update(crc, data, strlen((const char *)data));
	uint32_t result = ~crc;
	/* Just verify it produces a non-zero value */
	ck_assert_uint_ne(result, 0U);
}
END_TEST

/* Test: CRC32C incremental update */
START_TEST(test_crc32c_incremental)
{
	const uint8_t part1[] = "hel";
	const uint8_t part2[] = "lo";

	/* All at once */
	uint32_t crc_all = ~0U;
	crc_all = crc32c_update(crc_all, part1, 3);
	crc_all = crc32c_update(crc_all, part2, 2);
	uint32_t result_all = ~crc_all;

	/* All together */
	uint8_t combined[] = "hello";
	uint32_t crc_combined = ~0U;
	crc_combined = crc32c_update(crc_combined, combined, 5);
	uint32_t result_combined = ~crc_combined;

	/* Both should match */
	ck_assert_uint_eq(result_all, result_combined);
}
END_TEST

/* Test: CRC32C of zeros */
START_TEST(test_crc32c_zeros)
{
	uint8_t data[64];
	memset(data, 0, sizeof(data));

	uint32_t crc = ~0U;
	crc = crc32c_update(crc, data, sizeof(data));
	uint32_t result = ~crc;

	/* Zero bytes should produce some value (not necessarily predictable) */
	ck_assert_uint_ne(result, 0U);
}
END_TEST

/* Test: CRC32C sensitivity — changing one byte changes result */
START_TEST(test_crc32c_sensitivity)
{
	uint8_t data1[] = "postgresql";
	uint8_t data2[] = "postgresqll";  /* changed last 'l' */

	uint32_t crc1 = ~0U;
	crc1 = crc32c_update(crc1, data1, strlen((const char *)data1));
	uint32_t result1 = ~crc1;

	uint32_t crc2 = ~0U;
	crc2 = crc32c_update(crc2, data2, strlen((const char *)data2));
	uint32_t result2 = ~crc2;

	/* Results must differ */
	ck_assert_uint_ne(result1, result2);
}
END_TEST

/* Test: CRC32C large buffer */
START_TEST(test_crc32c_large)
{
	uint8_t data[16384];
	/* Fill with pattern */
	for (int i = 0; i < 16384; i++)
		data[i] = (uint8_t)(i % 256);

	uint32_t crc = ~0U;
	crc = crc32c_update(crc, data, sizeof(data));
	uint32_t result = ~crc;

	/* Just verify it doesn't crash and produces a value */
	ck_assert_uint_ne(result, 0U);
}
END_TEST

/* Create test suite for CRC32C */
Suite *
crc32c_suite(void)
{
	Suite *s;
	TCase *tc_core;

	s = suite_create("CRC32C");

	tc_core = tcase_create("core");
	tcase_add_test(tc_core, test_crc32c_empty);
	tcase_add_test(tc_core, test_crc32c_simple);
	tcase_add_test(tc_core, test_crc32c_incremental);
	tcase_add_test(tc_core, test_crc32c_zeros);
	tcase_add_test(tc_core, test_crc32c_sensitivity);
	tcase_add_test(tc_core, test_crc32c_large);
	suite_add_tcase(s, tc_core);

	return s;
}

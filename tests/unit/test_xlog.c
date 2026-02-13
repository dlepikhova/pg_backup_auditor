/*
 * test_xlog.c
 *
 * Unit tests for LSN and WAL utilities
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
 * Test: Parse valid LSN format
 */
START_TEST(test_parse_lsn_valid)
{
	XLogRecPtr lsn;
	bool result;

	/* Test standard format */
	result = parse_lsn("0/F000028", &lsn);
	ck_assert(result == true);
	ck_assert_uint_eq(lsn, 0x0F000028);

	/* Test with larger upper part */
	result = parse_lsn("1/2000000", &lsn);
	ck_assert(result == true);
	ck_assert_uint_eq(lsn, 0x102000000);

	/* Test zero LSN */
	result = parse_lsn("0/0", &lsn);
	ck_assert(result == true);
	ck_assert_uint_eq(lsn, 0);

	/* Test uppercase hex */
	result = parse_lsn("ABCD/EF123456", &lsn);
	ck_assert(result == true);
	ck_assert_uint_eq(lsn, 0xABCDEF123456);
}
END_TEST

/*
 * Test: Parse invalid LSN formats
 */
START_TEST(test_parse_lsn_invalid)
{
	XLogRecPtr lsn;
	bool result;

	/* Missing slash */
	result = parse_lsn("0F000028", &lsn);
	ck_assert(result == false);

	/* Empty string */
	result = parse_lsn("", &lsn);
	ck_assert(result == false);

	/* NULL pointer */
	result = parse_lsn(NULL, &lsn);
	ck_assert(result == false);

	/* Invalid hex characters */
	result = parse_lsn("0/G000028", &lsn);
	ck_assert(result == false);

	/* Extra characters */
	result = parse_lsn("0/F000028extra", &lsn);
	ck_assert(result == false);
}
END_TEST

/*
 * Test: Parse real-world LSN values
 */
START_TEST(test_parse_lsn_real_world)
{
	XLogRecPtr lsn;
	bool result;

	/* From pg_probackup backup.control */
	result = parse_lsn("0/100000B8", &lsn);
	ck_assert(result == true);
	ck_assert_uint_eq(lsn, 0x0100000B8);

	/* From another backup */
	result = parse_lsn("0/2000028", &lsn);
	ck_assert(result == true);
	ck_assert_uint_eq(lsn, 0x02000028);
}
END_TEST

/*
 * Test: Convert LSN to WAL segment
 */
START_TEST(test_lsn_to_seg_basic)
{
	WALSegmentName seg;

	/* Test LSN at start of first segment
	 * LSN 0x0 / 16MB (0x1000000) = segment 0 */
	lsn_to_seg(0x0, 1, &seg, 0x1000000);
	ck_assert_uint_eq(seg.timeline, 1);
	ck_assert_uint_eq(seg.log_id, 0);
	ck_assert_uint_eq(seg.seg_id, 0);

	/* Test LSN in third segment
	 * LSN 0x2000028 / 16MB = segment 2 */
	lsn_to_seg(0x2000028, 1, &seg, 0x1000000);
	ck_assert_uint_eq(seg.timeline, 1);
	ck_assert_uint_eq(seg.log_id, 0);
	ck_assert_uint_eq(seg.seg_id, 2);

	/* Test LSN at start of second segment
	 * LSN 0x1000000 / 16MB = segment 1 */
	lsn_to_seg(0x1000000, 1, &seg, 0x1000000);
	ck_assert_uint_eq(seg.timeline, 1);
	ck_assert_uint_eq(seg.log_id, 0);
	ck_assert_uint_eq(seg.seg_id, 1);
}
END_TEST

/*
 * Test: Convert LSN to WAL segment with different timelines
 */
START_TEST(test_lsn_to_seg_timeline)
{
	WALSegmentName seg;

	/* Same LSN, different timeline */
	lsn_to_seg(0x1000000, 2, &seg, 0x1000000);
	ck_assert_uint_eq(seg.timeline, 2);
	ck_assert_uint_eq(seg.log_id, 0);
	ck_assert_uint_eq(seg.seg_id, 1);
}
END_TEST

/*
 * Test: Convert LSN to WAL segment with overflow
 */
START_TEST(test_lsn_to_seg_overflow)
{
	WALSegmentName seg;

	/* Test large LSN that causes log_id increment
	 * Segment number = LSN / 16MB
	 * log_id = segment_number / 0x100000000
	 * seg_id = segment_number % 0x100000000
	 */

	/* LSN that results in segment 0x100000001 */
	uint64_t large_lsn = 0x100000001ULL * 0x1000000ULL;
	lsn_to_seg(large_lsn, 1, &seg, 0x1000000);
	ck_assert_uint_eq(seg.timeline, 1);
	ck_assert_uint_eq(seg.log_id, 1);
	ck_assert_uint_eq(seg.seg_id, 1);
}
END_TEST

/*
 * Test Suite
 */
Suite*
xlog_suite(void)
{
	Suite *s;
	TCase *tc_core;
	TCase *tc_conversion;

	s = suite_create("XLog");

	/* LSN parsing tests */
	tc_core = tcase_create("Core");
	tcase_add_test(tc_core, test_parse_lsn_valid);
	tcase_add_test(tc_core, test_parse_lsn_invalid);
	tcase_add_test(tc_core, test_parse_lsn_real_world);
	suite_add_tcase(s, tc_core);

	/* LSN to segment conversion tests */
	tc_conversion = tcase_create("Conversion");
	tcase_add_test(tc_conversion, test_lsn_to_seg_basic);
	tcase_add_test(tc_conversion, test_lsn_to_seg_timeline);
	tcase_add_test(tc_conversion, test_lsn_to_seg_overflow);
	suite_add_tcase(s, tc_conversion);

	return s;
}

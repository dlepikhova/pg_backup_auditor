/*
 * test_ini_parser.c
 *
 * Unit tests for INI parser
 */

#include <check.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "ini_parser.h"

/* Test parsing a simple INI file */
START_TEST(test_ini_parse_simple)
{
	IniFile *ini;
	const char *value;
	FILE *fp;
	const char *test_file = "/tmp/test_ini_simple.ini";

	/* Create test file */
	fp = fopen(test_file, "w");
	ck_assert_ptr_nonnull(fp);
	fprintf(fp, "[section1]\n");
	fprintf(fp, "key1=value1\n");
	fprintf(fp, "key2=value2\n");
	fprintf(fp, "\n");
	fprintf(fp, "[section2]\n");
	fprintf(fp, "key3=value3\n");
	fclose(fp);

	/* Parse file */
	ini = ini_parse_file(test_file);
	ck_assert_ptr_nonnull(ini);

	/* Check values */
	value = ini_get_value(ini, "section1", "key1");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "value1");

	value = ini_get_value(ini, "section1", "key2");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "value2");

	value = ini_get_value(ini, "section2", "key3");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "value3");

	/* Check non-existent key */
	value = ini_get_value(ini, "section1", "nonexistent");
	ck_assert_ptr_null(value);

	/* Check non-existent section */
	value = ini_get_value(ini, "nonexistent", "key1");
	ck_assert_ptr_null(value);

	ini_free(ini);
	unlink(test_file);
}
END_TEST

/* Test parsing INI file with quoted values */
START_TEST(test_ini_parse_quoted)
{
	IniFile *ini;
	const char *value;
	FILE *fp;
	const char *test_file = "/tmp/test_ini_quoted.ini";

	/* Create test file */
	fp = fopen(test_file, "w");
	ck_assert_ptr_nonnull(fp);
	fprintf(fp, "[backrest]\n");
	fprintf(fp, "backrest-format=5\n");
	fprintf(fp, "backrest-version=\"2.59.0dev\"\n");
	fclose(fp);

	/* Parse file */
	ini = ini_parse_file(test_file);
	ck_assert_ptr_nonnull(ini);

	/* Check values */
	value = ini_get_value(ini, "backrest", "backrest-format");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "5");

	value = ini_get_value(ini, "backrest", "backrest-version");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "2.59.0dev");

	ini_free(ini);
	unlink(test_file);
}
END_TEST

/* Test parsing INI file with comments */
START_TEST(test_ini_parse_comments)
{
	IniFile *ini;
	const char *value;
	FILE *fp;
	const char *test_file = "/tmp/test_ini_comments.ini";

	/* Create test file */
	fp = fopen(test_file, "w");
	ck_assert_ptr_nonnull(fp);
	fprintf(fp, "# This is a comment\n");
	fprintf(fp, "[section1]\n");
	fprintf(fp, "; Another comment\n");
	fprintf(fp, "key1=value1\n");
	fprintf(fp, "# Comment in section\n");
	fprintf(fp, "key2=value2\n");
	fclose(fp);

	/* Parse file */
	ini = ini_parse_file(test_file);
	ck_assert_ptr_nonnull(ini);

	/* Check values */
	value = ini_get_value(ini, "section1", "key1");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "value1");

	value = ini_get_value(ini, "section1", "key2");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "value2");

	ini_free(ini);
	unlink(test_file);
}
END_TEST

/* Test integer parsing */
START_TEST(test_ini_get_int)
{
	IniFile *ini;
	int value;
	FILE *fp;
	const char *test_file = "/tmp/test_ini_int.ini";

	/* Create test file */
	fp = fopen(test_file, "w");
	ck_assert_ptr_nonnull(fp);
	fprintf(fp, "[section1]\n");
	fprintf(fp, "number=42\n");
	fprintf(fp, "negative=-10\n");
	fclose(fp);

	/* Parse file */
	ini = ini_parse_file(test_file);
	ck_assert_ptr_nonnull(ini);

	/* Check values */
	value = ini_get_int(ini, "section1", "number", 0);
	ck_assert_int_eq(value, 42);

	value = ini_get_int(ini, "section1", "negative", 0);
	ck_assert_int_eq(value, -10);

	/* Check default value */
	value = ini_get_int(ini, "section1", "nonexistent", 99);
	ck_assert_int_eq(value, 99);

	ini_free(ini);
	unlink(test_file);
}
END_TEST

/* Test boolean parsing */
START_TEST(test_ini_get_bool)
{
	IniFile *ini;
	bool value;
	FILE *fp;
	const char *test_file = "/tmp/test_ini_bool.ini";

	/* Create test file */
	fp = fopen(test_file, "w");
	ck_assert_ptr_nonnull(fp);
	fprintf(fp, "[section1]\n");
	fprintf(fp, "bool1=true\n");
	fprintf(fp, "bool2=false\n");
	fprintf(fp, "bool3=1\n");
	fprintf(fp, "bool4=0\n");
	fprintf(fp, "bool5=yes\n");
	fprintf(fp, "bool6=no\n");
	fclose(fp);

	/* Parse file */
	ini = ini_parse_file(test_file);
	ck_assert_ptr_nonnull(ini);

	/* Check values */
	value = ini_get_bool(ini, "section1", "bool1", false);
	ck_assert(value == true);

	value = ini_get_bool(ini, "section1", "bool2", true);
	ck_assert(value == false);

	value = ini_get_bool(ini, "section1", "bool3", false);
	ck_assert(value == true);

	value = ini_get_bool(ini, "section1", "bool4", true);
	ck_assert(value == false);

	value = ini_get_bool(ini, "section1", "bool5", false);
	ck_assert(value == true);

	value = ini_get_bool(ini, "section1", "bool6", true);
	ck_assert(value == false);

	/* Check default value */
	value = ini_get_bool(ini, "section1", "nonexistent", true);
	ck_assert(value == true);

	ini_free(ini);
	unlink(test_file);
}
END_TEST

/* Test parsing INI file with whitespace */
START_TEST(test_ini_parse_whitespace)
{
	IniFile *ini;
	const char *value;
	FILE *fp;
	const char *test_file = "/tmp/test_ini_whitespace.ini";

	/* Create test file with various whitespace */
	fp = fopen(test_file, "w");
	ck_assert_ptr_nonnull(fp);
	fprintf(fp, "  [section1]  \n");
	fprintf(fp, "  key1  =  value1  \n");
	fprintf(fp, "key2=value2\n");
	fclose(fp);

	/* Parse file */
	ini = ini_parse_file(test_file);
	ck_assert_ptr_nonnull(ini);

	/* Check values - whitespace should be trimmed */
	value = ini_get_value(ini, "section1", "key1");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "value1");

	value = ini_get_value(ini, "section1", "key2");
	ck_assert_ptr_nonnull(value);
	ck_assert_str_eq(value, "value2");

	ini_free(ini);
	unlink(test_file);
}
END_TEST

Suite *ini_parser_suite(void)
{
	Suite *s;
	TCase *tc_core;

	s = suite_create("INI Parser");

	tc_core = tcase_create("Core");
	tcase_add_test(tc_core, test_ini_parse_simple);
	tcase_add_test(tc_core, test_ini_parse_quoted);
	tcase_add_test(tc_core, test_ini_parse_comments);
	tcase_add_test(tc_core, test_ini_get_int);
	tcase_add_test(tc_core, test_ini_get_bool);
	tcase_add_test(tc_core, test_ini_parse_whitespace);

	suite_add_tcase(s, tc_core);

	return s;
}

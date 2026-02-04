# Unit Tests

Unit tests for pg_backup_auditor using libcheck framework.

## Prerequisites

- libcheck (install via `brew install check` on macOS)
- C11 compiler

## Running Tests

From project root:
```bash
make test
```

From tests/unit directory:
```bash
make run
```

## Test Coverage

### String Utilities (`test_string_utils.c`)
- `str_trim()` - 6 tests
  - Leading whitespace
  - Trailing whitespace
  - Both leading and trailing
  - Only whitespace
  - Empty string
  - No whitespace

- `str_copy()` - 4 tests
  - Normal copy
  - Truncation
  - Exact fit
  - Empty string

### Adapter Registry (`test_adapter_registry.c`)
- `backup_type_to_string()` - 5 tests
  - FULL, PAGE, DELTA, PTRACK
  - Unknown type

- `backup_tool_to_string()` - 3 tests
  - pg_basebackup, pg_probackup
  - Unknown tool

- `backup_status_to_string()` - 7 tests
  - OK, RUNNING, CORRUPT, ERROR, ORPHAN, WARNING
  - Unknown status

## Adding New Tests

1. Create new test file in `tests/unit/`
2. Implement test suite function (see existing tests for examples)
3. Add suite declaration in `test_runner.c`
4. Add suite to runner in `test_runner.c` main function
5. Add source files to `Makefile` if testing new modules

## Test Structure

Each test file follows this pattern:

```c
#include <check.h>
#include "pg_backup_auditor.h"

START_TEST(test_function_name)
{
    // Test code
    ck_assert_int_eq(expected, actual);
    ck_assert_str_eq(expected, actual);
}
END_TEST

Suite *module_suite(void)
{
    Suite *s = suite_create("Module Name");
    TCase *tc = tcase_create("function_name");

    tcase_add_test(tc, test_function_name);
    suite_add_tcase(s, tc);

    return s;
}
```

### pg_basebackup Adapter (`test_pg_basebackup.c`)
- `pg_basebackup_detect()` - 4 tests
  - Plain format detection
  - Tar format detection
  - Invalid backup detection
  - Non-existent directory handling

- `pg_basebackup_scan()` - 3 tests
  - Plain format scanning
  - Tar format scanning
  - Missing backup_label handling

- `pg_basebackup_read_metadata()` - 4 tests
  - backup_id generation from timestamp
  - LSN parsing
  - Timeline parsing
  - Default node_name

## Current Test Statistics

- **Total Tests**: 36
- **Failures**: 0
- **Pass Rate**: 100%

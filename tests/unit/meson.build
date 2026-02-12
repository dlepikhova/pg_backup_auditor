# Unit tests

# Common source files needed for tests
test_common_sources = files(
  '../../src/common/string_utils.c',
  '../../src/common/logging.c',
  '../../src/common/file_utils.c',
  '../../src/common/xlog.c',
  '../../src/common/ini_parser.c',
  '../../src/scanner/fs_scanner.c',
  '../../src/adapters/adapter_registry.c',
  '../../src/adapters/pg_basebackup.c',
  '../../src/adapters/pg_probackup.c',
  '../../src/adapters/pgbackrest.c',
)

# Test sources
test_sources = files(
  'test_runner.c',
  'test_string_utils.c',
  'test_adapter_registry.c',
  'test_pg_basebackup.c',
  'test_pgbackrest.c',
  'test_sorting.c',
  'test_xlog.c',
  'test_ini_parser.c',
)

# Test-specific compiler flags (disable Werror for libcheck compatibility)
test_c_args = c_args + ['-Wno-error=gnu-zero-variadic-macro-arguments']

# Build test runner
test_runner = executable('test_runner',
  test_sources + test_common_sources,
  include_directories: [inc_dirs, inc_postgresql],
  c_args: test_c_args,
  dependencies: [check_dep, deps],
)

# Register test
test('unit tests', test_runner)

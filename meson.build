# Meson build file for pg_backup_auditor
# Phase 2 build system

project('pg_backup_auditor', 'c',
  version: '0.1.0-dev',
  license: 'GPL-3.0-or-later',
  default_options: [
    'c_std=c11',
    'warning_level=3',
    'werror=true',
    'optimization=2',
  ]
)

# Project metadata
project_description = 'PostgreSQL backup auditor - unified analysis and validation tool'

# Configuration
conf_data = configuration_data()
conf_data.set_quoted('VERSION', meson.project_version())
conf_data.set_quoted('PROJECT_NAME', meson.project_name())

# Platform detection
host_system = host_machine.system()

# Find PostgreSQL
pg_config = find_program('pg_config', required: false)

if pg_config.found()
  pg_includedir = run_command(pg_config, '--includedir', check: true).stdout().strip()
  pg_includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()

  inc_postgresql = [
    include_directories(pg_includedir),
    include_directories(pg_includedir_server),
  ]

  message('Found PostgreSQL:')
  message('  Include dir: ' + pg_includedir)
  message('  Server include dir: ' + pg_includedir_server)
else
  warning('pg_config not found - building without PostgreSQL headers')
  inc_postgresql = []
endif

# Include directories
inc_dirs = include_directories('include')

# Source files
sources = files(
  'src/main.c',
  'src/cli/cmd_list.c',
  'src/cli/cmd_check.c',
  'src/cli/cmd_info.c',
  'src/cli/cmd_help.c',
  'src/common/xlog.c',
  'src/common/logging.c',
  'src/common/string_utils.c',
  'src/common/file_utils.c',
  'src/common/arg_parser.c',
  'src/common/ini_parser.c',
  'src/scanner/fs_scanner.c',
  'src/adapters/pg_basebackup.c',
  'src/adapters/pg_probackup.c',
  'src/adapters/pgbackrest.c',
  'src/adapters/adapter_registry.c',
  'src/validator/backup_validator.c',
  'src/validator/wal_validator.c',
)

# Compiler flags
c_args = []

# Platform-specific flags
if host_system == 'darwin'
  c_args += ['-DMACOS']
elif host_system == 'linux'
  c_args += ['-DLINUX']
elif host_system == 'freebsd'
  c_args += ['-DFREEBSD']
endif

# Dependencies (optional)
# zlib for compressed backups (optional)
zlib_dep = dependency('zlib', required: false)
if zlib_dep.found()
  c_args += ['-DHAVE_ZLIB']
  message('Found zlib - compressed backup support enabled')
endif

# json-c for JSON output (optional)
jsonc_dep = dependency('json-c', required: false)
if jsonc_dep.found()
  c_args += ['-DHAVE_JSON_C']
  message('Found json-c - JSON output support enabled')
endif

# libyaml for YAML output (optional)
yaml_dep = dependency('yaml-0.1', required: false)
if yaml_dep.found()
  c_args += ['-DHAVE_YAML']
  message('Found yaml - YAML output support enabled')
endif

# Collect all dependencies
deps = []
if zlib_dep.found()
  deps += zlib_dep
endif
if jsonc_dep.found()
  deps += jsonc_dep
endif
if yaml_dep.found()
  deps += yaml_dep
endif

# Main executable
executable('pg_backup_auditor',
  sources,
  include_directories: [inc_dirs, inc_postgresql],
  c_args: c_args,
  dependencies: deps,
  install: true,
)

# Installation paths
install_data('README.md',
  install_dir: get_option('datadir') / 'doc' / 'pg_backup_auditor'
)

install_data('docs/INSTALL.md',
  install_dir: get_option('datadir') / 'doc' / 'pg_backup_auditor'
)

install_data('docs/DEVELOPER.md',
  install_dir: get_option('datadir') / 'doc' / 'pg_backup_auditor'
)

# Summary
summary({
  'prefix': get_option('prefix'),
  'bindir': get_option('bindir'),
  'PostgreSQL': pg_config.found() ? 'yes' : 'no',
  'zlib': zlib_dep.found() ? 'yes' : 'no',
  'json-c': jsonc_dep.found() ? 'yes' : 'no',
  'yaml': yaml_dep.found() ? 'yes' : 'no',
}, section: 'Configuration')

# Tests
subdir('tests')

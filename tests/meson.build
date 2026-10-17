# Tests subdirectory

# Check dependency
check_dep = dependency('check', required: false)

if check_dep.found()
  subdir('unit')
else
  warning('libcheck not found - unit tests disabled')
  warning('Install libcheck: brew install check (macOS) or apt-get install check (Linux)')
endif

# CLI tests: pytest against the built binary; skipped when pytest is missing
pymod = import('python')
python3 = pymod.find_installation('python3', modules: ['pytest'], required: false)

if python3.found()
  test('cli', python3,
    args: ['-m', 'pytest', '-p', 'no:cacheprovider', meson.project_source_root() / 'tests' / 'cli'],
    env: {'PG_BACKUP_AUDITOR': auditor_exe.full_path()},
    depends: auditor_exe,
    workdir: meson.project_source_root(),
  )
else
  warning('Python 3 with pytest not found - CLI tests disabled')
  warning('Install: python3 -m venv .venv && .venv/bin/pip install -r tests/requirements.txt')
endif

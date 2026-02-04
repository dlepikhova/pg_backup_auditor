# Tests subdirectory

# Check dependency
check_dep = dependency('check', required: false)

if check_dep.found()
  subdir('unit')
else
  warning('libcheck not found - unit tests disabled')
  warning('Install libcheck: brew install check (macOS) or apt-get install check (Linux)')
endif

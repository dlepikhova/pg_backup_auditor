# Makefile for pg_backup_auditor
# MVP (Phase 1)

# Detect platform
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Platform-specific defaults
ifeq ($(UNAME_S),Darwin)
    ifeq ($(UNAME_M),arm64)
        PREFIX ?= /opt/homebrew
    else
        PREFIX ?= /usr/local
    endif
    CC = clang
else ifeq ($(UNAME_S),Linux)
    PREFIX ?= /usr/local
    CC ?= gcc
else ifeq ($(UNAME_S),FreeBSD)
    PREFIX ?= /usr/local
    CC = clang
endif

# Compiler flags
CFLAGS = -std=c11 -Wall -Wextra -Werror -O2
CFLAGS += -I./include

# Linker flags
LDFLAGS = -lm

# Debug build support
DEBUG ?= 0
ifeq ($(DEBUG),1)
    CFLAGS += -g -O0 -DDEBUG
else
    CFLAGS += -DNDEBUG
endif

# Source files
SRCS = src/main.c \
       src/cli/cmd_list.c \
       src/cli/cmd_check.c \
       src/cli/cmd_info.c \
       src/cli/cmd_audit.c \
       src/cli/cmd_stat.c \
       src/cli/cmd_help.c \
       src/common/xlog.c \
       src/common/logging.c \
       src/common/string_utils.c \
       src/common/file_utils.c \
       src/common/crc32c.c \
       src/common/arg_parser.c \
       src/common/backup_chain.c \
       src/common/ini_parser.c \
       src/common/sha256.c \
       src/scanner/fs_scanner.c \
       src/adapters/pg_basebackup.c \
       src/adapters/pg_probackup.c \
       src/adapters/pgbackrest.c \
       src/adapters/adapter_registry.c \
       src/validator/backup_validator.c \
       src/validator/wal_validator.c \
       src/validator/pg_probackup_validator.c \
       src/validator/pg_basebackup_validator.c \
       src/validator/pgbackrest_validator.c

# Object files
OBJS = $(SRCS:.c=.o)

# Target binary
TARGET = pg_backup_auditor

# Default target
all: $(TARGET)

# Link
$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

# Compile
%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# Install
install: $(TARGET)
	install -d $(PREFIX)/bin
	install -m 755 $(TARGET) $(PREFIX)/bin/

# Uninstall
uninstall:
	rm -f $(PREFIX)/bin/$(TARGET)

# Clean
clean:
	rm -f $(OBJS) $(TARGET)
	@cd tests/unit && $(MAKE) clean 2>/dev/null || true

# Clean all (including dependencies)
distclean: clean
	rm -f .depend

# Generate dependencies
depend:
	$(CC) $(CFLAGS) -MM $(SRCS) > .depend

# Unit tests
test:
	@echo "Running unit tests..."
	@cd tests/unit && $(MAKE) run

# Alias for CI compatibility
test-unit: test

# Code coverage (requires gcov/lcov)
test-coverage:
	@echo "Running tests with coverage..."
	@cd tests/unit && $(MAKE) clean
	@cd tests/unit && $(MAKE) COVERAGE=1 run
	@echo "Generating coverage report..."
	@mkdir -p coverage
	@lcov --capture --directory tests/unit --output-file coverage/coverage.info || echo "Warning: lcov not found, skipping coverage report"
	@lcov --remove coverage/coverage.info '/usr/*' --output-file coverage/coverage.info || true
	@genhtml coverage/coverage.info --output-directory coverage || echo "Warning: genhtml not found, skipping HTML report"
	@echo "Coverage report generated in coverage/index.html"

# CLI tests (pytest) against the built binary
PYTHON ?= python3
test-cli: all
	PG_BACKUP_AUDITOR=$(CURDIR)/$(TARGET) $(PYTHON) -m pytest -p no:cacheprovider tests/cli

# Unit tests with sanitizers (Linux: ASan + UBSan, macOS: UBSan only).
# Test objects share paths with the main
# build (src/*.o), so rebuild from clean before and after.
test-sanitize:
	@cd tests/unit && $(MAKE) clean && $(MAKE) SANITIZE=1 run
	@cd tests/unit && $(MAKE) clean

# Meson build directory used by check-all
MESON_BUILDDIR ?= build/meson-check

# Everything to run before push: both build systems and unit tests
check-all:
	$(MAKE) clean
	$(MAKE)
	$(MAKE) test-cli
	$(MAKE) test
	@if [ -d $(MESON_BUILDDIR) ]; then meson setup --reconfigure $(MESON_BUILDDIR); \
	 else meson setup $(MESON_BUILDDIR); fi
	meson compile -C $(MESON_BUILDDIR)
	meson test -C $(MESON_BUILDDIR) --print-errorlogs

# Clean tests
test-clean:
	@cd tests/unit && $(MAKE) clean
	@rm -rf coverage

# Include dependencies if they exist
-include .depend

# Phony targets
.PHONY: all install uninstall clean distclean depend test test-unit test-coverage test-cli test-sanitize check-all test-clean

# Help
help:
	@echo "pg_backup_auditor Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all        - Build the project (default)"
	@echo "  install    - Install to $(PREFIX)/bin"
	@echo "  uninstall  - Remove from $(PREFIX)/bin"
	@echo "  clean      - Remove build artifacts"
	@echo "  distclean  - Remove all generated files"
	@echo "  depend     - Generate dependencies"
	@echo "  test       - Run unit tests"
	@echo "  test-cli   - Run CLI tests (needs Python 3 + pytest)"
	@echo "  test-sanitize - Run unit tests with sanitizers (ASan on Linux only)"
	@echo "  check-all  - Makefile + meson builds and tests (run before push)"
	@echo "  help       - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX     - Installation prefix (default: $(PREFIX))"
	@echo "  DEBUG      - Enable debug build (DEBUG=1)"
	@echo "  CC         - C compiler (default: $(CC))"

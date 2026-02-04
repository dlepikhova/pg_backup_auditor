# Makefile for pg_backup_auditor
# MVP (Phase 1)

# Detect platform
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Platform-specific defaults
ifeq ($(UNAME_S),Darwin)
    ifeq ($(UNAME_M),arm64)
        PREFIX ?= /opt/homebrew
        PG_CONFIG ?= /opt/homebrew/bin/pg_config
    else
        PREFIX ?= /usr/local
        PG_CONFIG ?= /usr/local/bin/pg_config
    endif
    CC = clang
else ifeq ($(UNAME_S),Linux)
    PREFIX ?= /usr/local
    PG_CONFIG ?= pg_config
    CC ?= gcc
else ifeq ($(UNAME_S),FreeBSD)
    PREFIX ?= /usr/local
    PG_CONFIG ?= /usr/local/bin/pg_config
    CC = clang
endif

# Compiler flags
CFLAGS = -std=c11 -Wall -Wextra -Werror -O2
CFLAGS += -I./include
CFLAGS += -I$(shell $(PG_CONFIG) --includedir)
CFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)

# Linker flags
LDFLAGS =

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
       src/cli/cmd_help.c \
       src/common/xlog.c \
       src/common/logging.c \
       src/common/string_utils.c \
       src/common/file_utils.c \
       src/common/arg_parser.c \
       src/scanner/fs_scanner.c \
       src/adapters/pg_basebackup.c \
       src/adapters/pg_probackup.c \
       src/adapters/adapter_registry.c \
       src/validator/backup_validator.c \
       src/validator/wal_validator.c

# Object files
OBJS = $(SRCS:.c=.o)

# Target binary
TARGET = pg_backup_auditor

# Default target
all: $(TARGET)

# Link
$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

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

# Clean tests
test-clean:
	@cd tests/unit && $(MAKE) clean

# Include dependencies if they exist
-include .depend

# Phony targets
.PHONY: all install uninstall clean distclean depend test test-clean

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
	@echo "  help       - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX     - Installation prefix (default: $(PREFIX))"
	@echo "  PG_CONFIG  - Path to pg_config (default: $(PG_CONFIG))"
	@echo "  DEBUG      - Enable debug build (DEBUG=1)"
	@echo "  CC         - C compiler (default: $(CC))"

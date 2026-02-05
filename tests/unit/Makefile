# Makefile for unit tests

# Compiler settings
CC = clang
CFLAGS = -std=c11 -Wall -Wextra -Werror -g
CFLAGS += -I../../include
# Suppress warning from libcheck headers about GNU extensions
CFLAGS += -Wno-gnu-zero-variadic-macro-arguments

# Coverage support
ifeq ($(COVERAGE),1)
    CFLAGS += --coverage
    LDFLAGS_COVERAGE = --coverage
endif

# Libraries
LDFLAGS = -lcheck -lm -lpthread $(LDFLAGS_COVERAGE)

# Check if pkg-config is available for libcheck
PKG_CONFIG := $(shell which pkg-config 2>/dev/null)
ifneq ($(PKG_CONFIG),)
    CHECK_CFLAGS := $(shell pkg-config --cflags check 2>/dev/null)
    CHECK_LDFLAGS := $(shell pkg-config --libs check 2>/dev/null)
    ifneq ($(CHECK_CFLAGS),)
        CFLAGS += $(CHECK_CFLAGS)
        LDFLAGS = $(CHECK_LDFLAGS) $(LDFLAGS_COVERAGE)
    endif
endif

# Source files from main project
COMMON_SRCS = ../../src/common/string_utils.c \
              ../../src/common/logging.c \
              ../../src/common/file_utils.c \
              ../../src/common/xlog.c \
              ../../src/adapters/adapter_registry.c \
              ../../src/adapters/pg_basebackup.c \
              ../../src/adapters/pg_probackup.c

# Test source files
TEST_SRCS = test_runner.c \
            test_string_utils.c \
            test_adapter_registry.c \
            test_pg_basebackup.c \
            test_sorting.c \
            test_xlog.c

# Object files
COMMON_OBJS = $(COMMON_SRCS:.c=.o)
TEST_OBJS = $(TEST_SRCS:.c=.o)

# Test executable
TEST_RUNNER = test_runner

.PHONY: all clean run

all: $(TEST_RUNNER)

$(TEST_RUNNER): $(TEST_OBJS) $(COMMON_OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(TEST_OBJS) $(COMMON_OBJS) $(TEST_RUNNER)

run: $(TEST_RUNNER)
	./$(TEST_RUNNER)

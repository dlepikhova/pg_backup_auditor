# Installation Guide

## Prerequisites

### All Platforms

- C11-compatible compiler (GCC 7+ or Clang 10+)
- Make
- PostgreSQL development headers (optional — for `pg_config` detection)

### macOS

```bash
# Install Xcode Command Line Tools
xcode-select --install

# Install PostgreSQL (choose one)
brew install postgresql@17

# For running tests (optional)
brew install check
```

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install build-essential postgresql-server-dev-17

# For running tests (optional)
sudo apt install check
```

### RHEL/Rocky/AlmaLinux

```bash
sudo dnf install gcc make postgresql17-devel

# For running tests (optional)
sudo dnf install check check-devel
```

### FreeBSD

```bash
sudo pkg install postgresql17-client gmake

# For running tests (optional)
sudo pkg install check
```

## Building from Source

### Standard Build

```bash
cd pg_backup_auditor
make
```

### Debug Build

```bash
make DEBUG=1
```

### Specify PostgreSQL Version

```bash
make PG_CONFIG=/path/to/pg_config
```

## Installation

### System-wide Installation

```bash
sudo make install PREFIX=/usr/local
```

### User Installation

```bash
make install PREFIX=$HOME/.local
```

Make sure `~/.local/bin` is in your PATH.

### Custom Installation

```bash
make install PREFIX=/opt/pg_backup_auditor
```

## Verification

```bash
pg_backup_auditor --version
pg_backup_auditor --help
```

## Uninstallation

```bash
sudo make uninstall PREFIX=/usr/local
```

## Troubleshooting

### pg_config not found

Set the `PG_CONFIG` variable:

```bash
make PG_CONFIG=/usr/pgsql-16/bin/pg_config
```

### Compilation errors on macOS

Ensure you're using Clang:

```bash
make CC=clang
```

### `check.h` not found (unit tests)

libcheck is required for `make test`. Install it:

```bash
# Ubuntu/Debian
sudo apt install check

# macOS
brew install check

# RHEL/Rocky/AlmaLinux
sudo dnf install check check-devel

# FreeBSD
sudo pkg install check
```

### Missing PostgreSQL headers

Install the development package for your PostgreSQL version.

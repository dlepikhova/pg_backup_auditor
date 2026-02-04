# Installation Guide

## Prerequisites

### All Platforms

- PostgreSQL 10+ development headers
- C11-compatible compiler
- Make

### macOS

```bash
# Install Xcode Command Line Tools
xcode-select --install

# Install PostgreSQL (choose one)
# Via Homebrew:
brew install postgresql@16

# Via Postgres.app:
# Download from https://postgresapp.com/
```

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install build-essential postgresql-server-dev-16
```

### RHEL/Rocky/AlmaLinux

```bash
sudo dnf install gcc make postgresql16-devel
```

### FreeBSD

```bash
sudo pkg install postgresql16-client gmake
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

### Missing PostgreSQL headers

Install the development package for your PostgreSQL version.

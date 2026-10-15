# Installation Guide

## Prerequisites

### All Platforms

- C11-compatible compiler (GCC 7+ or Clang 10+)
- Make

PostgreSQL itself is not needed: the auditor reads backup files directly and does not use PostgreSQL headers or libraries.

### macOS

```bash
# Install Xcode Command Line Tools
xcode-select --install

# For running tests (optional)
brew install check
```

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install build-essential

# For running tests (optional)
sudo apt install check
```

### RHEL/Rocky/AlmaLinux

```bash
sudo dnf install gcc make

# For running tests (optional)
sudo dnf install check check-devel
```

### FreeBSD

```bash
sudo pkg install gmake

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

# Meson Build System

pg_backup_auditor supports two build systems:
- **Makefile** (Phase 1 - MVP)
- **Meson** (Phase 2 - Recommended)

## Why Meson?

Meson provides:
- ✅ Faster builds with Ninja backend
- ✅ Better dependency management
- ✅ Automatic detection of optional libraries
- ✅ Cross-compilation support
- ✅ Modern, declarative syntax
- ✅ Built-in testing framework

## Prerequisites

### Install Meson

**macOS**:
```bash
brew install meson ninja
```

**Ubuntu/Debian**:
```bash
sudo apt install meson ninja-build
```

**RHEL/Fedora**:
```bash
sudo dnf install meson ninja-build
```

**FreeBSD**:
```bash
sudo pkg install meson ninja
```

## Quick Start

### Setup and Build

```bash
# Setup build directory
meson setup builddir

# Compile
meson compile -C builddir

# Run tests (when implemented)
meson test -C builddir

# Install
sudo meson install -C builddir
```

### Build Options

```bash
# Debug build
meson setup builddir --buildtype=debug

# Release build with optimizations
meson setup builddir --buildtype=release

# Custom prefix
meson setup builddir --prefix=/usr/local

# Reconfigure
meson setup builddir --reconfigure
```

## Configuration Summary

After setup, Meson shows a configuration summary:

```
pg_backup_auditor 0.1.0-dev

  Configuration
    prefix    : /opt/homebrew
    bindir    : bin
    PostgreSQL: yes
    zlib      : yes
    json-c    : no
    yaml      : no
```

### Optional Dependencies

Meson automatically detects and enables optional features:

- **PostgreSQL** (`pg_config`): Required for PostgreSQL headers
- **zlib**: Compressed backup support
- **json-c**: JSON output format
- **yaml**: YAML output format

### Installing Dependencies

**macOS**:
```bash
brew install postgresql zlib json-c libyaml
```

**Ubuntu/Debian**:
```bash
sudo apt install postgresql-server-dev-all zlib1g-dev libjson-c-dev libyaml-dev
```

## Build Targets

```bash
# Compile only
meson compile -C builddir

# Clean build
rm -rf builddir
meson setup builddir

# Verbose build
meson compile -C builddir -v

# Show build options
meson configure builddir
```

## Installation

### System-wide Installation

```bash
sudo meson install -C builddir
```

Default locations (with `--prefix=/usr/local`):
- Binary: `/usr/local/bin/pg_backup_auditor`
- Documentation: `/usr/local/share/doc/pg_backup_auditor/`

### User Installation

```bash
meson setup builddir --prefix=$HOME/.local
meson compile -C builddir
meson install -C builddir
```

Make sure `~/.local/bin` is in your `PATH`.

### Custom Installation

```bash
meson setup builddir --prefix=/opt/pg_backup_auditor
meson install -C builddir
```

### Dry Run

Test installation without actually installing:

```bash
meson install -C builddir --dry-run
```

## Cross-Compilation

Meson supports cross-compilation with cross-files:

```bash
# Example: Build for Linux on macOS
meson setup builddir --cross-file cross/linux-x86_64.txt
```

## IDE Integration

Meson generates IDE project files:

```bash
# VS Code
meson setup builddir --backend=vs

# Xcode
meson setup builddir --backend=xcode
```

## Comparison: Makefile vs Meson

| Feature | Makefile | Meson |
|---------|----------|-------|
| Setup | None | `meson setup builddir` |
| Build | `make` | `meson compile -C builddir` |
| Install | `make install` | `meson install -C builddir` |
| Clean | `make clean` | `rm -rf builddir` |
| Parallel build | `make -j$(nproc)` | Automatic with Ninja |
| Dependency detection | Manual | Automatic |
| Speed | Moderate | Fast (Ninja) |
| Out-of-tree build | No | Yes (by default) |

## Common Tasks

### Rebuild After Changes

```bash
meson compile -C builddir
```

Meson automatically detects changed files.

### Change Build Options

```bash
meson configure builddir -Dbuildtype=debug
meson compile -C builddir
```

### View Build Logs

```bash
cat builddir/meson-logs/meson-log.txt
```

### Uninstall

Meson doesn't have built-in uninstall, but you can:

```bash
# See what would be installed
meson install -C builddir --dry-run

# Manually remove files
sudo rm /usr/local/bin/pg_backup_auditor
sudo rm -rf /usr/local/share/doc/pg_backup_auditor
```

## Testing

Unit tests are implemented and integrated with Meson:

```bash
# Run all tests
meson test -C builddir

# Run with verbose output
meson test -C builddir -v

# Run specific test suite
meson test -C builddir "unit tests"

# Run tests in parallel
meson test -C builddir -j 4

# Show test output even on success
meson test -C builddir --print-errorlogs
```

Current test coverage: **~68% overall**
- 45+ unit tests covering adapters, utilities, and core functionality
- Tests for pg_basebackup and pg_probackup adapters
- Tests for sorting, xlog utilities, and string utilities

## Benchmarking

```bash
# Run benchmarks
meson test -C builddir --benchmark

# Profile build
meson compile -C builddir --profile
```

## Troubleshooting

### pg_config not found

```bash
# Specify pg_config location
export PATH="/usr/pgsql-16/bin:$PATH"
meson setup builddir --reconfigure
```

### Dependencies not found

```bash
# Install missing dependencies
brew install zlib json-c libyaml  # macOS

# Or disable optional features by not installing them
# Meson will automatically skip unavailable dependencies
```

### Clean rebuild

```bash
rm -rf builddir
meson setup builddir
meson compile -C builddir
```

## Advanced Usage

### Environment Variables

```bash
# Custom C compiler
CC=gcc meson setup builddir

# Custom flags
CFLAGS="-march=native" meson setup builddir

# Custom pg_config
PG_CONFIG=/usr/pgsql-16/bin/pg_config meson setup builddir
```

### Build Types

- `debug`: Debug build with symbols, no optimization
- `debugoptimized`: Debug symbols + optimization (default)
- `release`: Full optimization, no debug symbols
- `minsize`: Optimize for size
- `plain`: No extra flags

```bash
meson setup builddir --buildtype=release
```

## Resources

- Meson Documentation: https://mesonbuild.com/
- Ninja Build: https://ninja-build.org/
- Meson Tutorial: https://mesonbuild.com/Tutorial.html

---

**Recommended**: Use Meson for development and production builds for better performance and maintainability.

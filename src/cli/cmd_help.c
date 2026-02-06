/*
 * cmd_help.c
 *
 * Help and usage message functions
 *
 * Copyright (C) 2026 Daria Lepikhova
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


#include "cmd_help.h"
#include <stdio.h>

/*
 * Print general usage/help message
 */
void
print_general_usage(void)
{
	printf("pg_backup_auditor - PostgreSQL backup auditing and validation tool\n\n");
	printf("Usage: pg_backup_auditor COMMAND [OPTIONS]\n\n");
	printf("COMMANDS:\n");
	printf("  list    - List available backups\n");
	printf("  info    - Show detailed backup information\n");
	printf("  check   - Validate backup consistency\n");
	printf("  help    - Show this help message\n\n");
	printf("Use 'pg_backup_auditor COMMAND --help' for command-specific options.\n\n");
}

/*
 * Print usage for 'list' command
 */
void
print_list_usage(void)
{
	printf("Usage: pg_backup_auditor list [OPTIONS]\n\n");
	printf("List available backups from backup directory.\n\n");

	printf("OPTIONS:\n");
	printf("  -B, --backup-dir=PATH    Path to backup directory (required)\n");
	printf("  -t, --tool=TOOL          Filter by backup tool (pg_basebackup, pg_probackup)\n");
	printf("  -s, --status=STATUS      Filter by status (ok, running, error, corrupt)\n");
	printf("  -n, --limit=N            Limit number of results (default: unlimited)\n");
	printf("  -o, --output=FORMAT      Output format: table, json, csv (default: table)\n");
	printf("  -h, --help               Show this help message\n\n");

	printf("EXAMPLES:\n");
	printf("  pg_backup_auditor list -B /backup/pg\n");
	printf("  pg_backup_auditor list -B /backup/pg --tool=pg_basebackup\n");
	printf("  pg_backup_auditor list -B /backup/pg --status=error --output=json\n\n");
}

/*
 * Print usage for 'info' command
 */
void
print_info_usage(void)
{
	printf("Usage: pg_backup_auditor info [OPTIONS]\n\n");
	printf("Display detailed information about a specific backup.\n\n");

	printf("OPTIONS:\n");
	printf("  -B, --backup-dir=PATH    Path to backup directory (required)\n");
	printf("  -i, --backup-id=ID       Backup ID to inspect (required)\n");
	printf("  -o, --output=FORMAT      Output format: text, json (default: text)\n");
	printf("  -h, --help               Show this help message\n\n");

	printf("INFORMATION DISPLAYED:\n");
	printf("  General      - Backup ID, type, tool, status\n");
	printf("  Timing       - Start time, end time, duration\n");
	printf("  Storage      - Size, path, compression\n");
	printf("  PostgreSQL   - Version, timeline, LSN range\n");
	printf("  WAL          - WAL segments needed for recovery\n\n");
}

/*
 * Print usage for 'check' command
 */
void
print_check_usage(void)
{
	printf("Usage: pg_backup_auditor check [OPTIONS]\n\n");
	printf("Validate backup consistency and check for issues.\n\n");

	printf("OPTIONS:\n");
	printf("  -B, --backup-dir=PATH    Path to backup directory (required)\n");
	printf("  -i, --backup-id=ID       Check specific backup by ID\n");
	printf("  -l, --level=LEVEL        Validation level (default: standard)\n");
	printf("                           Levels: basic, standard, checksums, full\n");
	printf("      --wal-archive=PATH   Path to external WAL archive (optional)\n");
	printf("      --skip-wal           Skip all WAL checking\n");
	printf("  -h, --help               Show this help message\n\n");

	printf("VALIDATION LEVELS:\n");
	printf("  basic      - Level 1: File structure + chain connectivity + WAL presence\n");
	printf("  standard   - Level 2: Level 1 + metadata validation (default)\n");
	printf("  checksums  - Level 3: Level 2 + checksums + WAL continuity\n");
	printf("  full       - Level 4: Level 3 + comprehensive checks (pg_verifybackup)\n\n");

	printf("CHECKS BY LEVEL:\n");
	printf("  Level 1 (basic):\n");
	printf("    - File structure: backup_label/backup_manifest/backup.control presence\n");
	printf("    - Directory structure: base/, global/, database/ as required\n");
	printf("    - Chain connectivity: incremental backups have valid parent\n");
	printf("    - WAL presence: required WAL files exist in backup\n\n");
	printf("  Level 2 (standard):\n");
	printf("    - Required metadata fields: backup_id, start_time, etc.\n");
	printf("    - LSN validity: start_lsn < stop_lsn\n");
	printf("    - Timestamp consistency: start_time < end_time\n");
	printf("    - Timeline and version presence\n\n");
	printf("  Level 3 (checksums):\n");
	printf("    - Checksum validation: verify file checksums\n");
	printf("    - WAL continuity: complete WAL segment chain from start_lsn to stop_lsn\n\n");
	printf("  Level 4 (full):\n");
	printf("    - pg_verifybackup: if available for pg_basebackup\n");
	printf("    - All comprehensive checks\n\n");

	printf("WAL CHECKING:\n");
	printf("  WAL checks are performed automatically if backup contains WAL metadata:\n");
	printf("    - Level 1: Checks for WAL files presence in backup directory\n");
	printf("    - Level 3: Validates WAL continuity using LSN range\n");
	printf("  Use --skip-wal to disable automatic WAL checks\n");
	printf("  Use --wal-archive to specify external WAL archive location\n\n");

	printf("EXIT CODES:\n");
	printf("  0 - All checks passed successfully\n");
	printf("  1 - General error (cannot scan directory, etc.)\n");
	printf("  2 - Validation issues found (errors or warnings)\n");
	printf("  4 - Invalid arguments\n\n");

	printf("EXAMPLES:\n");
	printf("  # Basic structure check\n");
	printf("  pg_backup_auditor check -B /backup/pg --level=basic\n\n");
	printf("  # Standard validation (default)\n");
	printf("  pg_backup_auditor check -B /backup/pg\n\n");
	printf("  # Full validation with WAL continuity\n");
	printf("  pg_backup_auditor check -B /backup/pg --level=full\n\n");
	printf("  # Check specific backup, skip WAL\n");
	printf("  pg_backup_auditor check -B /backup/pg -i 20240101T120000 --skip-wal\n\n");
	printf("  # Use external WAL archive\n");
	printf("  pg_backup_auditor check -B /backup/pg --wal-archive=/wal/archive\n\n");
}

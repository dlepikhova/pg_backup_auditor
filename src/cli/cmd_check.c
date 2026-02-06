/*
 * cmd_check.c
 *
 * Implementation of 'check' command
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


#include "pg_backup_auditor.h"
#include "cmd_help.h"
#include "arg_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

/* Command-line options */
typedef struct {
	char *backup_dir;
	char *backup_id;
	char *wal_archive;
	ValidationLevel level;
	bool skip_wal;
} CheckOptions;

static void
init_options(CheckOptions *opts)
{
	opts->backup_dir = NULL;
	opts->backup_id = NULL;
	opts->wal_archive = NULL;
	opts->level = VALIDATION_LEVEL_STANDARD;  /* default: level 2 */
	opts->skip_wal = false;
}

static int
parse_arguments(int argc, char **argv, CheckOptions *opts)
{
	int c;
	int option_index = 0;
	bool backup_dir_seen = false;
	bool backup_id_seen = false;
	bool wal_archive_seen = false;
	bool level_seen = false;
	bool skip_wal_seen = false;

	static struct option long_options[] = {
		{"backup-dir",      required_argument, 0, 'B'},
		{"backup-id",       required_argument, 0, 'i'},
		{"wal-archive",     required_argument, 0, 'w'},
		{"level",           required_argument, 0, 'l'},
		{"skip-wal",        no_argument,       0, 'S'},
		{"help",            no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "B:i:w:l:h",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'B':
				if (check_duplicate_option(backup_dir_seen, "--backup-dir"))
					return EXIT_INVALID_ARGUMENTS;
				opts->backup_dir = optarg;
				backup_dir_seen = true;
				break;
			case 'i':
				if (check_duplicate_option(backup_id_seen, "--backup-id"))
					return EXIT_INVALID_ARGUMENTS;
				opts->backup_id = optarg;
				backup_id_seen = true;
				break;
			case 'w':
				if (check_duplicate_option(wal_archive_seen, "--wal-archive"))
					return EXIT_INVALID_ARGUMENTS;
				opts->wal_archive = optarg;
				wal_archive_seen = true;
				break;
			case 'l':
				if (check_duplicate_option(level_seen, "--level"))
					return EXIT_INVALID_ARGUMENTS;
				if (strcmp(optarg, "basic") == 0)
					opts->level = VALIDATION_LEVEL_BASIC;
				else if (strcmp(optarg, "standard") == 0)
					opts->level = VALIDATION_LEVEL_STANDARD;
				else if (strcmp(optarg, "checksums") == 0)
					opts->level = VALIDATION_LEVEL_CHECKSUMS;
				else if (strcmp(optarg, "full") == 0)
					opts->level = VALIDATION_LEVEL_FULL;
				else
				{
					fprintf(stderr, "Error: Invalid validation level: %s\n", optarg);
					fprintf(stderr, "Valid levels: basic, standard, checksums, full\n");
					return EXIT_INVALID_ARGUMENTS;
				}
				level_seen = true;
				break;
			case 'S':
				if (check_duplicate_option(skip_wal_seen, "--skip-wal"))
					return EXIT_INVALID_ARGUMENTS;
				opts->skip_wal = true;
				skip_wal_seen = true;
				break;
			case 'h':
				print_check_usage();
				return EXIT_SUCCESS;
			case '?':
				return EXIT_INVALID_ARGUMENTS;
			default:
				return EXIT_INVALID_ARGUMENTS;
		}
	}

	return -1;  /* Continue processing */
}

static int
validate_options(const CheckOptions *opts)
{
	/* backup-dir is required */
	if (!validate_required_option(opts->backup_dir, "--backup-dir"))
		return EXIT_INVALID_ARGUMENTS;

	/* Validate paths exist */
	if (opts->backup_dir != NULL && !is_directory(opts->backup_dir))
	{
		fprintf(stderr, "Error: Backup directory does not exist: %s\n", opts->backup_dir);
		return EXIT_GENERAL_ERROR;
	}

	if (opts->wal_archive != NULL && !is_directory(opts->wal_archive))
	{
		fprintf(stderr, "Error: WAL archive directory does not exist: %s\n", opts->wal_archive);
		return EXIT_GENERAL_ERROR;
	}

	return EXIT_SUCCESS;
}

/*
 * cmd_check_main - Main function for the 'check' command
 *
 * Performs validation and consistency checks of backups and WAL archive.
 *
 * Return codes:
 * - EXIT_SUCCESS (0) if all checks pass successfully
 * - 2 (EXIT_VALIDATION_FAILED) if validation issues are detected
 * - EXIT_FAILURE (1) on critical error
 * - 4 (EXIT_INVALID_ARGUMENTS) on invalid arguments
 */
int
cmd_check_main(int argc, char **argv)
{
	CheckOptions opts;
	BackupInfo *backups = NULL;
	int ret;
	int total_errors = 0;
	int total_warnings = 0;

	/* Initialize logging */
	log_init();

	/* Initialize options */
	init_options(&opts);

	/* Parse command-line arguments */
	ret = parse_arguments(argc, argv, &opts);
	if (ret == EXIT_SUCCESS)  /* --help was shown */
		return EXIT_SUCCESS;
	if (ret != -1)  /* Error occurred */
		return ret;

	/* Validate options */
	ret = validate_options(&opts);
	if (ret != EXIT_SUCCESS)
		return ret;

	/* Scan backup directory */
	log_info("Scanning backup directory: %s", opts.backup_dir);
	backups = scan_backup_directory(opts.backup_dir, -1);

	if (backups == NULL)
	{
		fprintf(stderr, "Error: No backups found in: %s\n", opts.backup_dir);
		return EXIT_NO_BACKUPS_FOUND;
	}

	/* Validate backups */
	const char *level_names[] = {"basic", "standard", "checksums", "full"};
	printf("====================================================\n");
	printf("Backup Validation\n");
	printf("====================================================\n");
	printf("Directory:        %s\n", opts.backup_dir);
	printf("Validation level: %s\n", level_names[opts.level]);
	printf("====================================================\n");

	BackupInfo *current = backups;
	int backup_count = 0;
	int backups_validated = 0;
	int backups_skipped = 0;

	while (current != NULL)
	{
		/* Filter by backup_id if specified */
		if (opts.backup_id != NULL &&
			strcmp(current->backup_id, opts.backup_id) != 0)
		{
			current = current->next;
			continue;
		}

		backup_count++;
		if (backup_count > 1)
			printf("\n");  /* Separator between backups */

		printf("%sBackup:%s %s (%s)\n",
			   use_color ? COLOR_BOLD : "", use_color ? COLOR_RESET : "",
			   current->backup_id, backup_tool_to_string(current->tool));

		/* Skip validation for backups with ERROR or CORRUPT status */
		if (current->status == BACKUP_STATUS_ERROR || current->status == BACKUP_STATUS_CORRUPT)
		{
			printf("  %s[SKIPPED]%s Status: %s - validation not performed\n",
				   use_color ? COLOR_CYAN : "", use_color ? COLOR_RESET : "",
				   backup_status_to_string(current->status));
			backups_skipped++;
			current = current->next;
			continue;
		}

		backups_validated++;

		/* Level 1: Basic structure checks + chain validation + WAL presence */
		if (opts.level >= VALIDATION_LEVEL_BASIC)
		{
			/* TODO: Add structure validation */
			log_debug("Structure check not yet implemented");

			/* TODO: Add chain validation */
			log_debug("Chain validation not yet implemented");

			/* WAL presence check (Level 1) - if backup has WAL metadata */
			if (!opts.skip_wal && current->wal_start_file[0] != '\0')
			{
				/* TODO: Check if required WAL files exist in backup */
				log_debug("WAL presence check not yet implemented");
			}
		}

		/* Level 2: Metadata validation */
		if (opts.level >= VALIDATION_LEVEL_STANDARD)
		{
			ValidationResult *metadata_result = validate_backup_metadata(current);
			if (metadata_result != NULL)
			{
				total_errors += metadata_result->error_count;
				total_warnings += metadata_result->warning_count;
				free_validation_result(metadata_result);
			}
		}

		/* Level 3: Checksums + WAL continuity */
		if (opts.level >= VALIDATION_LEVEL_CHECKSUMS)
		{
			/* TODO: Add checksum validation */
			log_debug("Checksum validation not yet implemented");

			/* WAL continuity check (Level 3) - if backup has LSN range */
			if (!opts.skip_wal && current->start_lsn > 0 && current->stop_lsn > 0)
			{
				/* TODO: Check WAL continuity from start_lsn to stop_lsn */
				log_debug("WAL continuity check not yet implemented");
			}
		}

		/* Level 4: Full validation (pg_verifybackup, etc.) */
		if (opts.level >= VALIDATION_LEVEL_FULL)
		{
			/* TODO: Add full validation (pg_verifybackup, etc.) */
			log_debug("Full validation not yet implemented");
		}

		current = current->next;
	}

	/* Print summary */
	printf("\n");
	printf("====================================================\n");
	printf("Validation Summary\n");
	printf("====================================================\n");
	printf("  Total backups found:    %d\n", backup_count);
	printf("  Backups validated:      %d\n", backups_validated);
	if (backups_skipped > 0)
		printf("  Backups skipped:        %d (ERROR/CORRUPT status)\n", backups_skipped);
	printf("----------------------------------------------------\n");
	printf("  Validation errors:      %d\n", total_errors);
	printf("  Validation warnings:    %d\n", total_warnings);
	printf("====================================================\n");

	/* Cleanup */
	free_backup_list(backups);

	/* Return appropriate exit code */
	if (total_errors > 0)
	{
		printf("\n%sResult: FAILED%s\n",
			   use_color ? COLOR_RED : "", use_color ? COLOR_RESET : "");
		printf("  %d validation error%s found in checked backups.\n",
			   total_errors, total_errors == 1 ? "" : "s");
		if (backups_skipped > 0)
			printf("  %d backup%s skipped due to ERROR/CORRUPT status.\n",
				   backups_skipped, backups_skipped == 1 ? " was" : "s were");
		return EXIT_VALIDATION_FAILED;
	}
	else if (total_warnings > 0)
	{
		printf("\n%sResult: WARNING%s\n",
			   use_color ? COLOR_YELLOW : "", use_color ? COLOR_RESET : "");
		printf("  %d validation warning%s found in checked backups.\n",
			   total_warnings, total_warnings == 1 ? "" : "s");
		if (backups_skipped > 0)
			printf("  %d backup%s skipped due to ERROR/CORRUPT status.\n",
				   backups_skipped, backups_skipped == 1 ? " was" : "s were");
		return EXIT_VALIDATION_FAILED;
	}
	else
	{
		if (backups_validated == 0 && backups_skipped > 0)
		{
			printf("\n%sResult: NO VALIDATION PERFORMED%s\n",
				   use_color ? COLOR_CYAN : "", use_color ? COLOR_RESET : "");
			printf("  All %d backup%s skipped (ERROR/CORRUPT status).\n",
				   backups_skipped, backups_skipped == 1 ? " was" : "s were");
			printf("  No backups were available for validation.\n");
		}
		else
		{
			printf("\n%sResult: OK%s\n",
				   use_color ? COLOR_GREEN : "", use_color ? COLOR_RESET : "");
			printf("  All %d validated backup%s passed checks successfully.\n",
				   backups_validated, backups_validated == 1 ? "" : "s");
			if (backups_skipped > 0)
				printf("  %d backup%s skipped due to ERROR/CORRUPT status.\n",
					   backups_skipped, backups_skipped == 1 ? " was" : "s were");
		}
		return EXIT_SUCCESS;
	}
}

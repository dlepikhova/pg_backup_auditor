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
#include "adapter.h"
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

/* ------------------------------------------------------------------ *
 * Chain grouping helpers
 * ------------------------------------------------------------------ */

typedef struct {
	BackupInfo  *root;      /* Root FULL backup; NULL = orphaned group */
	BackupInfo **members;   /* Sorted by start_time, oldest first */
	int          count;
	int          capacity;
} BackupChain;

static BackupInfo *
find_backup_in_list(BackupInfo *list, const char *id)
{
	for (BackupInfo *b = list; b != NULL; b = b->next)
		if (strcmp(b->backup_id, id) == 0)
			return b;
	return NULL;
}

/* Walk up parent links to find the root FULL backup */
static BackupInfo *
find_chain_root(BackupInfo *backup, BackupInfo *all_backups)
{
	BackupInfo *cur = backup;
	for (int depth = 0; depth < 1000; depth++)
	{
		if (cur->type == BACKUP_TYPE_FULL)
			return cur;
		if (cur->parent_backup_id[0] == '\0')
			return NULL;
		cur = find_backup_in_list(all_backups, cur->parent_backup_id);
		if (cur == NULL)
			return NULL;
	}
	return NULL;  /* cycle guard */
}

static int
compare_backup_by_time(const void *a, const void *b)
{
	time_t ta = (*(const BackupInfo **)a)->start_time;
	time_t tb = (*(const BackupInfo **)b)->start_time;
	return (ta > tb) - (ta < tb);
}

static bool
chain_append(BackupChain *chain, BackupInfo *backup)
{
	if (chain->count == chain->capacity)
	{
		int nc = chain->capacity ? chain->capacity * 2 : 8;
		BackupInfo **nm = realloc(chain->members, nc * sizeof(*nm));
		if (nm == NULL)
			return false;
		chain->members = nm;
		chain->capacity = nc;
	}
	chain->members[chain->count++] = backup;
	return true;
}

/*
 * build_chains — group all_backups by chain root.
 *
 * Each FULL backup starts a new chain.  Incrementals are assigned to the
 * chain of their root FULL.  Backups with no reachable FULL ancestor go
 * into the orphaned bucket (root = NULL).
 *
 * Returns a calloc'd array of BackupChain; *nchains is set to the length.
 * The orphaned bucket is always last; it is included only if non-empty.
 */
static BackupChain *
build_chains(BackupInfo *all_backups, int *nchains)
{
	int full_count = 0;
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
		if (b->type == BACKUP_TYPE_FULL)
			full_count++;

	BackupChain *chains = calloc(full_count + 1, sizeof(BackupChain));
	if (chains == NULL)
		return NULL;

	int ci = 0;

	/* One chain per FULL */
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
	{
		if (b->type != BACKUP_TYPE_FULL)
			continue;
		chains[ci].root = b;
		chain_append(&chains[ci], b);
		ci++;
	}

	/* Assign non-FULL backups to their chain or the orphaned bucket */
	int orphan = full_count;
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
	{
		if (b->type == BACKUP_TYPE_FULL)
			continue;

		BackupInfo *root   = find_chain_root(b, all_backups);
		int         target = orphan;

		if (root != NULL)
		{
			for (int i = 0; i < full_count; i++)
			{
				if (chains[i].root == root)
				{
					target = i;
					break;
				}
			}
		}
		chain_append(&chains[target], b);
	}

	/* Include orphaned bucket only if non-empty */
	int total = full_count;
	if (chains[orphan].count > 0)
		total++;

	/* Sort members within each chain by start_time */
	for (int i = 0; i < total; i++)
	{
		if (chains[i].count > 1)
			qsort(chains[i].members, chains[i].count,
				  sizeof(BackupInfo *), compare_backup_by_time);
	}

	*nchains = total;
	return chains;
}

static void
free_chains(BackupChain *chains, int count)
{
	if (chains == NULL)
		return;
	for (int i = 0; i < count; i++)
		free(chains[i].members);
	free(chains);
}

/* Print [OK]/[ERROR]/[WARNING] results for one backup */
static void
print_validation_result(ValidationResult *result, const char *indent)
{
	for (int i = 0; i < result->error_count; i++)
		printf("%s  %s[ERROR]%s %s\n",
			   indent,
			   use_color ? COLOR_RED   : "", use_color ? COLOR_RESET : "",
			   result->errors[i]);
	for (int i = 0; i < result->warning_count; i++)
		printf("%s  %s[WARNING]%s %s\n",
			   indent,
			   use_color ? COLOR_YELLOW : "", use_color ? COLOR_RESET : "",
			   result->warnings[i]);
	if (result->error_count == 0 && result->warning_count == 0)
		printf("%s  %s[OK]%s Backup validation: passed\n",
			   indent,
			   use_color ? COLOR_GREEN : "", use_color ? COLOR_RESET : "");
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
	WALArchiveInfo *wal_info = NULL;
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

	/* Scan WAL archive if provided and WAL checks are not skipped */
	if (!opts.skip_wal && opts.wal_archive != NULL &&
		opts.level >= VALIDATION_LEVEL_CHECKSUMS)
	{
		log_info("Scanning WAL archive: %s", opts.wal_archive);
		wal_info = scan_wal_archive(opts.wal_archive);
		if (wal_info == NULL)
			log_warning("Failed to scan WAL archive: %s", opts.wal_archive);
		else
			log_info("Found %d WAL segments in archive", wal_info->segment_count);
	}

	/* Auto-detect WAL archive for archive-mode backups (stream=false)
	 * if --wal-archive was not explicitly provided */
	if (!opts.skip_wal && wal_info == NULL &&
		opts.level >= VALIDATION_LEVEL_CHECKSUMS)
	{
		BackupInfo *scan = backups;
		while (scan != NULL)
		{
			if (!scan->wal_stream && scan->start_lsn > 0)
			{
				BackupAdapter *adapter = get_adapter_for_tool(scan->tool);
				if (adapter != NULL && adapter->get_wal_archive_path != NULL)
				{
					char *wal_path = adapter->get_wal_archive_path(
						scan->backup_path, scan->instance_name);
					if (wal_path != NULL)
					{
						log_info("Auto-detected WAL archive: %s", wal_path);
						wal_info = scan_wal_archive(wal_path);
						free(wal_path);
						if (wal_info != NULL)
							log_info("Found %d WAL segments in archive",
									 wal_info->segment_count);
						break;
					}
				}
			}
			scan = scan->next;
		}
	}

	/* Validate backups */
	const char *level_names[] = {"basic", "standard", "checksums", "full"};
	printf("====================================================\n");
	printf("Backup Validation\n");
	printf("====================================================\n");
	printf("Directory:        %s\n", opts.backup_dir);
	printf("Validation level: %s\n", level_names[opts.level]);
	printf("====================================================\n");

	int backup_count = 0;
	int backups_validated = 0;
	int backups_skipped = 0;

	if (opts.backup_id != NULL)
	{
		/*
		 * Single-backup mode (--backup-id): validate one backup without
		 * chain grouping.  The user wants to focus on a specific backup.
		 */
		for (BackupInfo *cur = backups; cur != NULL; cur = cur->next)
		{
			if (strcmp(cur->backup_id, opts.backup_id) != 0)
				continue;

			backup_count++;
			printf("\n%sBackup:%s %s (%s)\n",
				   use_color ? COLOR_BOLD : "", use_color ? COLOR_RESET : "",
				   cur->backup_id, backup_tool_to_string(cur->tool));

			if (cur->status == BACKUP_STATUS_ERROR ||
				cur->status == BACKUP_STATUS_CORRUPT)
			{
				printf("  %s[SKIPPED]%s Status: %s - validation not performed\n",
					   use_color ? COLOR_CYAN  : "", use_color ? COLOR_RESET : "",
					   backup_status_to_string(cur->status));
				backups_skipped++;
			}
			else
			{
				backups_validated++;
				WALArchiveInfo   *chain_wal = opts.skip_wal ? NULL : wal_info;
				ValidationResult *result    =
					validate_backup_chain(cur, backups, chain_wal, opts.level);
				if (result != NULL)
				{
					print_validation_result(result, "");
					total_errors   += result->error_count;
					total_warnings += result->warning_count;
					free_validation_result(result);
				}
			}
			break;
		}
	}
	else
	{
		/*
		 * Chain-grouped mode: group backups by FULL root, validate and
		 * display each chain as a unit.
		 */
		int          nchains = 0;
		BackupChain *chains  = build_chains(backups, &nchains);

		if (chains == NULL)
		{
			fprintf(stderr, "Error: Failed to build backup chains\n");
			free_backup_list(backups);
			if (wal_info != NULL)
				free_wal_archive_info(wal_info);
			return EXIT_GENERAL_ERROR;
		}

		for (int ci = 0; ci < nchains; ci++)
		{
			BackupChain *chain       = &chains[ci];
			bool         is_orphaned = (chain->root == NULL);

			/* Chain header */
			printf("\n");
			if (is_orphaned)
			{
				printf("Orphaned Backups\n");
			}
			else
			{
				char date_str[32] = "N/A";
				if (chain->root->start_time > 0)
					strftime(date_str, sizeof(date_str), "%Y-%m-%d",
							 localtime(&chain->root->start_time));

				int incr_count = chain->count - 1;
				printf("Chain: %s  %s  %s",
					   chain->root->backup_id,
					   backup_tool_to_string(chain->root->tool),
					   date_str);
				if (incr_count > 0)
					printf("  (+%d incremental%s)",
						   incr_count, incr_count == 1 ? "" : "s");
				printf("\n");
			}
			printf("----------------------------------------------------\n");

			for (int mi = 0; mi < chain->count; mi++)
			{
				BackupInfo *cur    = chain->members[mi];
				bool        is_incr = (cur->type != BACKUP_TYPE_FULL && !is_orphaned);
				const char *indent = is_incr ? "  " : "";

				backup_count++;
				printf("\n");

				printf("%s%sBackup:%s %s (%s)",
					   indent,
					   use_color ? COLOR_BOLD  : "", use_color ? COLOR_RESET : "",
					   cur->backup_id,
					   backup_type_to_string(cur->type));
				if (cur->parent_backup_id[0] != '\0')
					printf("  ->  parent: %s", cur->parent_backup_id);
				printf("\n");

				if (cur->status == BACKUP_STATUS_ERROR ||
					cur->status == BACKUP_STATUS_CORRUPT)
				{
					printf("%s  %s[SKIPPED]%s Status: %s - validation not performed\n",
						   indent,
						   use_color ? COLOR_CYAN  : "", use_color ? COLOR_RESET : "",
						   backup_status_to_string(cur->status));
					backups_skipped++;
					continue;
				}

				backups_validated++;

				WALArchiveInfo   *chain_wal = opts.skip_wal ? NULL : wal_info;
				ValidationResult *result    =
					validate_backup_chain(cur, backups, chain_wal, opts.level);
				if (result != NULL)
				{
					print_validation_result(result, indent);
					total_errors   += result->error_count;
					total_warnings += result->warning_count;
					free_validation_result(result);
				}
			}
		}

		free_chains(chains, nchains);
	}

	/* WAL archive-wide continuity check (once, not per-backup) */
	if (!opts.skip_wal && wal_info != NULL &&
		opts.level >= VALIDATION_LEVEL_CHECKSUMS)
	{
		printf("\n");
		printf("WAL Archive\n");
		printf("----------------------------------------------------\n");

		/* Check whether any backup in the chain uses stream WAL.
		 * Stream backups do not store bridge segments (WAL between
		 * consecutive backups), so missing bridges are expected. */
		bool chain_has_stream = false;
		for (BackupInfo *b = backups; b != NULL; b = b->next)
		{
			if (b->wal_stream)
			{
				chain_has_stream = true;
				break;
			}
		}

		ValidationResult *chain_result = check_wal_restore_chain(backups, wal_info);
		if (chain_result != NULL)
		{
			if (chain_result->error_count > 0)
			{
				printf("  %s[ERROR]%s WAL restore chain incomplete:\n",
					   use_color ? COLOR_RED : "", use_color ? COLOR_RESET : "");
				for (int i = 0; i < chain_result->error_count; i++)
					printf("          %s\n", chain_result->errors[i]);
				if (chain_has_stream)
					printf("  %s[NOTE]%s Chain contains stream backups — bridge segments "
						   "between backups are not stored in stream mode and will not "
						   "appear in the WAL archive\n",
						   use_color ? COLOR_YELLOW : "", use_color ? COLOR_RESET : "");
			}
			else
			{
				printf("  %s[OK]%s WAL restore chain: all inter-backup bridges complete\n",
					   use_color ? COLOR_GREEN : "", use_color ? COLOR_RESET : "");
			}
			total_errors += chain_result->error_count;
			free_validation_result(chain_result);
		}

		ValidationResult *cont_result = check_wal_continuity(wal_info);
		if (cont_result != NULL)
		{
			if (cont_result->error_count > 0)
			{
				printf("  %s[ERROR]%s WAL archive has gaps:\n",
					   use_color ? COLOR_RED : "", use_color ? COLOR_RESET : "");
				for (int i = 0; i < cont_result->error_count; i++)
					printf("          %s\n", cont_result->errors[i]);
			}
			else
			{
				printf("  %s[OK]%s WAL archive: no gaps detected (%d segment%s)\n",
					   use_color ? COLOR_GREEN : "", use_color ? COLOR_RESET : "",
					   wal_info->segment_count,
					   wal_info->segment_count == 1 ? "" : "s");
			}
			total_errors += cont_result->error_count;
			free_validation_result(cont_result);
		}

		/* Archive-wide header validation — catches segment swaps and other
		 * corruptions in segments that lie between backups and are therefore
		 * not covered by the per-backup check_wal_headers() pass. */
		if (opts.level >= VALIDATION_LEVEL_FULL)
		{
			ValidationResult *arch_result = check_wal_archive_headers(wal_info);
			if (arch_result != NULL)
			{
				if (arch_result->error_count > 0)
				{
					printf("  %s[ERROR]%s WAL archive header validation failed:\n",
						   use_color ? COLOR_RED : "", use_color ? COLOR_RESET : "");
					for (int i = 0; i < arch_result->error_count; i++)
						printf("          %s\n", arch_result->errors[i]);
				}
				else
				{
					printf("  %s[OK]%s WAL archive: all segment headers valid\n",
						   use_color ? COLOR_GREEN : "", use_color ? COLOR_RESET : "");
				}
				total_errors += arch_result->error_count;
				free_validation_result(arch_result);
			}
		}
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
	if (wal_info != NULL)
		free_wal_archive_info(wal_info);

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

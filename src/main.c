/*
 * main.c
 *
 * Main entry point for pg_backup_auditor
 *
 * Copyright (C) 2026  Daria
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Command handlers (to be implemented) */
extern int cmd_list_main(int argc, char **argv);
extern int cmd_check_main(int argc, char **argv);
extern int cmd_info_main(int argc, char **argv);

static void
print_version(void)
{
	printf("pg_backup_auditor %s\n", PG_BACKUP_AUDITOR_VERSION);
}


int
main(int argc, char **argv)
{
	int ret = EXIT_SUCCESS;

	/* Initialize */
	pg_backup_auditor_init();

	/* Check for --no-color option anywhere in arguments */
	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--no-color") == 0)
		{
			use_color = false;
			break;
		}
	}

	/* Check for arguments */
	if (argc < 2)
	{
		print_general_usage();
		ret = EXIT_INVALID_ARGUMENTS;
		goto cleanup;
	}

	/* Handle global options */
	if (strcmp(argv[1], "--version") == 0)
	{
		print_version();
		goto cleanup;
	}

	if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)
	{
		print_general_usage();
		goto cleanup;
	}

	/* Dispatch to command handlers */
	if (strcmp(argv[1], "list") == 0)
	{
		ret = cmd_list_main(argc - 1, argv + 1);
	}
	else if (strcmp(argv[1], "check") == 0)
	{
		ret = cmd_check_main(argc - 1, argv + 1);
	}
	else if (strcmp(argv[1], "info") == 0)
	{
		ret = cmd_info_main(argc - 1, argv + 1);
	}
	else
	{
		fprintf(stderr, "Error: Unknown command '%s'\n\n", argv[1]);
		print_general_usage();
		ret = EXIT_INVALID_ARGUMENTS;
	}

cleanup:
	/* Cleanup */
	pg_backup_auditor_cleanup();

	return ret;
}

/* Initialization and cleanup stubs */
void
pg_backup_auditor_init(void)
{
	/* TODO: Initialize logging, etc. */
}

void
pg_backup_auditor_cleanup(void)
{
	/* TODO: Cleanup resources */
}

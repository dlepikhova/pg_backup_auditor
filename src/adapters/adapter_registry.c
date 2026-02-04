/*
 * adapter_registry.c
 *
 * Adapter registration and discovery
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Registry of all adapters */
BackupAdapter *pg_backup_auditor_adapters[] = {
	&pg_basebackup_adapter,
	&pg_probackup_adapter,
	NULL  /* sentinel */
};

/*
 * Detect backup type by trying each adapter
 */
BackupAdapter*
detect_backup_type(const char *path)
{
	int i;

	for (i = 0; pg_backup_auditor_adapters[i] != NULL; i++)
	{
		if (pg_backup_auditor_adapters[i]->detect(path))
			return pg_backup_auditor_adapters[i];
	}

	return NULL;
}

/*
 * Convert BackupType to string
 */
const char*
backup_type_to_string(BackupType type)
{
	switch (type)
	{
		case BACKUP_TYPE_FULL:
			return "FULL";
		case BACKUP_TYPE_INCREMENTAL:
			return "INCREMENTAL";
		case BACKUP_TYPE_PAGE:
			return "PAGE";
		case BACKUP_TYPE_DELTA:
			return "DELTA";
		case BACKUP_TYPE_PTRACK:
			return "PTRACK";
		default:
			return "UNKNOWN";
	}
}

/*
 * Convert BackupTool to string
 */
const char*
backup_tool_to_string(BackupTool tool)
{
	switch (tool)
	{
		case BACKUP_TOOL_PG_BASEBACKUP:
			return "pg_basebackup";
		case BACKUP_TOOL_PG_PROBACKUP:
			return "pg_probackup";
		default:
			return "unknown";
	}
}

/*
 * Convert BackupStatus to string
 */
const char*
backup_status_to_string(BackupStatus status)
{
	switch (status)
	{
		case BACKUP_STATUS_OK:
			return "OK";
		case BACKUP_STATUS_RUNNING:
			return "RUNNING";
		case BACKUP_STATUS_CORRUPT:
			return "CORRUPT";
		case BACKUP_STATUS_ERROR:
			return "ERROR";
		case BACKUP_STATUS_ORPHAN:
			return "ORPHAN";
		case BACKUP_STATUS_WARNING:
			return "WARNING";
		default:
			return "UNKNOWN";
	}
}

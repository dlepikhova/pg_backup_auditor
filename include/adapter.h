/*
 * adapter.h
 *
 * Backup adapter interface
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


#ifndef ADAPTER_H
#define ADAPTER_H

#include "types.h"
#include <stdbool.h>

/* Backup adapter interface */
typedef struct BackupAdapter {
	const char *name;

	/* Detect if a path contains a backup of this type */
	bool (*detect)(const char *path);

	/* Scan and parse backup metadata */
	BackupInfo* (*scan)(const char *backup_root);
	int (*read_metadata)(const char *backup_path, BackupInfo *info);

	/* Validate backup */
	ValidationResult* (*validate)(BackupInfo *info, WALArchiveInfo *wal);

	/* Cleanup */
	void (*cleanup)(BackupInfo *info);
} BackupAdapter;

/* Adapter registration */
extern BackupAdapter *pg_backup_auditor_adapters[];

/* Concrete adapters */
extern BackupAdapter pg_basebackup_adapter;
extern BackupAdapter pg_probackup_adapter;
extern BackupAdapter pgbackrest_adapter;

/* Helper functions */
BackupAdapter* detect_backup_type(const char *path);
const char* backup_type_to_string(BackupType type);
const char* backup_tool_to_string(BackupTool tool);
const char* backup_status_to_string(BackupStatus status);

#endif /* ADAPTER_H */

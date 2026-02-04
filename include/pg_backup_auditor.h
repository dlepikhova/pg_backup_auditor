/*
 * pg_backup_auditor.h
 *
 * Main header file for pg_backup_auditor
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


#ifndef PG_BACKUP_AUDITOR_H
#define PG_BACKUP_AUDITOR_H

#include "types.h"
#include "constants.h"
#include "adapter.h"
#include "common.h"

/* Version information */
#define PG_BACKUP_AUDITOR_VERSION "0.1.0-dev"
#define PG_BACKUP_AUDITOR_VERSION_MAJOR 0
#define PG_BACKUP_AUDITOR_VERSION_MINOR 1
#define PG_BACKUP_AUDITOR_VERSION_PATCH 0

/* Global initialization and cleanup */
void pg_backup_auditor_init(void);
void pg_backup_auditor_cleanup(void);

#endif /* PG_BACKUP_AUDITOR_H */

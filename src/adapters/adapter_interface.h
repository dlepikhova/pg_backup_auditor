/*
 * adapter_interface.h
 *
 * Internal header for backup adapters
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


#ifndef ADAPTER_INTERFACE_H
#define ADAPTER_INTERFACE_H

#include "pg_backup_auditor.h"

/* External adapter declarations */
extern BackupAdapter pg_basebackup_adapter;
extern BackupAdapter pg_probackup_adapter;

#endif /* ADAPTER_INTERFACE_H */

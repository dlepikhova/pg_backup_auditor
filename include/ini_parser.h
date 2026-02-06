/*
 * ini_parser.h
 *
 * INI file parser for pgBackRest configuration files
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

#ifndef INI_PARSER_H
#define INI_PARSER_H

#include <stdbool.h>

/* Maximum line length in INI file */
#define INI_MAX_LINE 8192

/* INI key-value pair */
typedef struct IniKeyValue {
	char *key;
	char *value;
	struct IniKeyValue *next;
} IniKeyValue;

/* INI section */
typedef struct IniSection {
	char *name;
	IniKeyValue *first_kv;
	struct IniSection *next;
} IniSection;

/* INI file structure */
typedef struct {
	char *filename;
	IniSection *first_section;
} IniFile;

/* Parse INI file from path */
IniFile* ini_parse_file(const char *filepath);

/* Get section by name */
IniSection* ini_get_section(IniFile *ini, const char *section_name);

/* Get value by section and key */
const char* ini_get_value(IniFile *ini, const char *section_name, const char *key);

/* Get integer value */
int ini_get_int(IniFile *ini, const char *section_name, const char *key, int default_value);

/* Get boolean value */
bool ini_get_bool(IniFile *ini, const char *section_name, const char *key, bool default_value);

/* Free INI file structure */
void ini_free(IniFile *ini);

#endif /* INI_PARSER_H */

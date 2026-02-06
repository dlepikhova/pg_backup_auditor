/*
 * ini_parser.c
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

#define _POSIX_C_SOURCE 200809L

#include "ini_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>

/*
 * Trim whitespace from both ends of string
 */
static char*
trim_whitespace(char *str)
{
	char *end;

	/* Trim leading space */
	while (isspace((unsigned char)*str))
		str++;

	if (*str == '\0')
		return str;

	/* Trim trailing space */
	end = str + strlen(str) - 1;
	while (end > str && isspace((unsigned char)*end))
		end--;

	/* Write new null terminator */
	end[1] = '\0';

	return str;
}

/*
 * Create new INI section
 */
static IniSection*
create_section(const char *name)
{
	IniSection *section;

	section = calloc(1, sizeof(IniSection));
	if (section == NULL)
		return NULL;

	section->name = strdup(name);
	if (section->name == NULL)
	{
		free(section);
		return NULL;
	}

	section->first_kv = NULL;
	section->next = NULL;

	return section;
}

/*
 * Add key-value pair to section
 */
static bool
add_key_value(IniSection *section, const char *key, const char *value)
{
	IniKeyValue *kv, *last;

	if (section == NULL || key == NULL || value == NULL)
		return false;

	kv = calloc(1, sizeof(IniKeyValue));
	if (kv == NULL)
		return false;

	kv->key = strdup(key);
	kv->value = strdup(value);
	kv->next = NULL;

	if (kv->key == NULL || kv->value == NULL)
	{
		free(kv->key);
		free(kv->value);
		free(kv);
		return false;
	}

	/* Add to end of list */
	if (section->first_kv == NULL)
	{
		section->first_kv = kv;
	}
	else
	{
		last = section->first_kv;
		while (last->next != NULL)
			last = last->next;
		last->next = kv;
	}

	return true;
}

/*
 * Parse INI file from path
 */
IniFile*
ini_parse_file(const char *filepath)
{
	FILE *fp;
	char line[INI_MAX_LINE];
	IniFile *ini;
	IniSection *current_section = NULL;
	IniSection *last_section = NULL;
	char *trimmed, *key, *value, *equals;

	if (filepath == NULL)
		return NULL;

	fp = fopen(filepath, "r");
	if (fp == NULL)
		return NULL;

	ini = calloc(1, sizeof(IniFile));
	if (ini == NULL)
	{
		fclose(fp);
		return NULL;
	}

	ini->filename = strdup(filepath);
	ini->first_section = NULL;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		trimmed = trim_whitespace(line);

		/* Skip empty lines and comments */
		if (trimmed[0] == '\0' || trimmed[0] == '#' || trimmed[0] == ';')
			continue;

		/* Check for section header */
		if (trimmed[0] == '[')
		{
			char *close_bracket = strchr(trimmed, ']');
			if (close_bracket != NULL)
			{
				*close_bracket = '\0';
				current_section = create_section(trimmed + 1);
				if (current_section == NULL)
				{
					fclose(fp);
					ini_free(ini);
					return NULL;
				}

				/* Add to section list */
				if (ini->first_section == NULL)
				{
					ini->first_section = current_section;
				}
				else
				{
					last_section->next = current_section;
				}
				last_section = current_section;
			}
			continue;
		}

		/* Parse key=value pair */
		equals = strchr(trimmed, '=');
		if (equals != NULL && current_section != NULL)
		{
			*equals = '\0';
			key = trim_whitespace(trimmed);
			value = trim_whitespace(equals + 1);

			/* Remove quotes from value if present */
			if (value[0] == '"')
			{
				value++;
				char *end_quote = strchr(value, '"');
				if (end_quote != NULL)
					*end_quote = '\0';
			}

			if (!add_key_value(current_section, key, value))
			{
				fclose(fp);
				ini_free(ini);
				return NULL;
			}
		}
	}

	fclose(fp);
	return ini;
}

/*
 * Get section by name
 */
IniSection*
ini_get_section(IniFile *ini, const char *section_name)
{
	IniSection *section;

	if (ini == NULL || section_name == NULL)
		return NULL;

	for (section = ini->first_section; section != NULL; section = section->next)
	{
		if (strcmp(section->name, section_name) == 0)
			return section;
	}

	return NULL;
}

/*
 * Get value by section and key
 */
const char*
ini_get_value(IniFile *ini, const char *section_name, const char *key)
{
	IniSection *section;
	IniKeyValue *kv;

	section = ini_get_section(ini, section_name);
	if (section == NULL)
		return NULL;

	for (kv = section->first_kv; kv != NULL; kv = kv->next)
	{
		if (strcmp(kv->key, key) == 0)
			return kv->value;
	}

	return NULL;
}

/*
 * Get integer value
 */
int
ini_get_int(IniFile *ini, const char *section_name, const char *key, int default_value)
{
	const char *value;

	value = ini_get_value(ini, section_name, key);
	if (value == NULL)
		return default_value;

	return atoi(value);
}

/*
 * Get boolean value
 */
bool
ini_get_bool(IniFile *ini, const char *section_name, const char *key, bool default_value)
{
	const char *value;

	value = ini_get_value(ini, section_name, key);
	if (value == NULL)
		return default_value;

	if (strcmp(value, "true") == 0 || strcmp(value, "1") == 0 || strcmp(value, "yes") == 0)
		return true;

	if (strcmp(value, "false") == 0 || strcmp(value, "0") == 0 || strcmp(value, "no") == 0)
		return false;

	return default_value;
}

/*
 * Free INI file structure
 */
void
ini_free(IniFile *ini)
{
	IniSection *section, *next_section;
	IniKeyValue *kv, *next_kv;

	if (ini == NULL)
		return;

	/* Free all sections */
	section = ini->first_section;
	while (section != NULL)
	{
		next_section = section->next;

		/* Free all key-value pairs in this section */
		kv = section->first_kv;
		while (kv != NULL)
		{
			next_kv = kv->next;
			free(kv->key);
			free(kv->value);
			free(kv);
			kv = next_kv;
		}

		free(section->name);
		free(section);
		section = next_section;
	}

	free(ini->filename);
	free(ini);
}

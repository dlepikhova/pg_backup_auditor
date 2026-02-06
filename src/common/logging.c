/*
 * logging.c
 *
 * Logging functionality
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
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>

static LogLevel current_log_level = LOG_INFO;
static FILE *log_file = NULL;

/* Global color support flag */
bool use_color = true;

/*
 * Initialize logging
 */
void
log_init(void)
{
	if (log_file == NULL)
		log_file = stderr;
}

/*
 * Set log level
 */
void
log_set_level(LogLevel level)
{
	current_log_level = level;
}

/*
 * Set log output file
 */
void
log_set_file(const char *filename)
{
	FILE *f = fopen(filename, "a");
	if (f != NULL)
	{
		if (log_file != NULL && log_file != stderr)
			fclose(log_file);
		log_file = f;
	}
}

/*
 * Generic logging function
 */
static void
log_message(LogLevel level, const char *fmt, va_list args)
{
	const char *level_str;
	time_t now;
	struct tm *tm_info;
	char time_buf[64];

	if (level < current_log_level)
		return;

	if (log_file == NULL)
		log_file = stderr;

	/* Level string */
	switch (level)
	{
		case LOG_DEBUG:
			level_str = "DEBUG";
			break;
		case LOG_INFO:
			level_str = "INFO";
			break;
		case LOG_WARNING:
			level_str = "WARNING";
			break;
		case LOG_ERROR:
			level_str = "ERROR";
			break;
		default:
			level_str = "UNKNOWN";
			break;
	}

	/* Timestamp if logging to file */
	if (log_file != stderr)
	{
		time(&now);
		tm_info = localtime(&now);
		strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
		fprintf(log_file, "[%s] ", time_buf);
	}

	fprintf(log_file, "[%s] ", level_str);
	vfprintf(log_file, fmt, args);
	fprintf(log_file, "\n");
	fflush(log_file);
}

/*
 * Logging functions for each level
 */
void
log_debug(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	log_message(LOG_DEBUG, fmt, args);
	va_end(args);
}

void
log_info(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	log_message(LOG_INFO, fmt, args);
	va_end(args);
}

void
log_warning(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	log_message(LOG_WARNING, fmt, args);
	va_end(args);
}

void
log_error(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	log_message(LOG_ERROR, fmt, args);
	va_end(args);
}

/*
 * Cleanup logging
 */
void
log_cleanup(void)
{
	if (log_file != NULL && log_file != stderr)
		fclose(log_file);
	log_file = NULL;
}

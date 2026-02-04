/*
 * cmd_help.h
 *
 * Help and usage message functions
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


#ifndef CMD_HELP_H
#define CMD_HELP_H

/*
 * Print general usage/help message
 */
void print_general_usage(void);

/*
 * Print usage for 'list' command
 */
void print_list_usage(void);

/*
 * Print usage for 'info' command
 */
void print_info_usage(void);

/*
 * Print usage for 'check' command
 */
void print_check_usage(void);

#endif /* CMD_HELP_H */

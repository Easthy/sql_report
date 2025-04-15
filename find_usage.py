import sys
import os
import re
import argparse
import json
import logging
from rich.logging import RichHandler
from rich.text import Text

import warnings

from sqlparse import parse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Name
from sql_metadata import Parser

logging.basicConfig(
    level=logging.INFO, handlers=[RichHandler(rich_tracebacks=True, show_time=False)]
)
logger = logging.getLogger("rich")


def find_sql_files(folder_path):
    """Finds all SQL files in the specified folder and subfolder"""
    return [
        os.path.join(root, file)
        for root, _, files in os.walk(folder_path)
        for file in files
        if file.endswith(".sql")
    ]


def get_file_content(file_path):
    """Reads the contents of the file and extracts the view name."""
    pattern = r"CREATE OR REPLACE VIEW (\w+\.\w+)"
    try:
        with open(file_path, "r", encoding="utf-8") as sql_file:
            content = sql_file.read()
            view_match = re.search(pattern, content)
            view_name = view_match.group(1) if view_match else None
            # Removing unnecessary lines
            sql_content = "".join(
                line
                for line in content.split("\n")
                if "WITH NO SCHEMA BINDING" not in line
                and "CREATE OR REPLACE VIEW" not in line
            )
            return sql_content, view_name
    except Exception as e:
        logger.error(f"Error reading the file {file_path}: {e}")
        return None, None


def find_column_usage(sql_content, target_schema, target_table, target_columns):
    """Searches for the usage of the specified columns from the table, taking aliases into account"""
    try:
        # Parsing SQL using sql_metadata.
        parser = Parser(sql_content)

        # Extended list of SQL keywords for filtering
        sql_keywords = {
            "SELECT",
            "FROM",
            "JOIN",
            "LEFT",
            "RIGHT",
            "INNER",
            "OUTER",
            "WHERE",
            "GROUP",
            "ORDER",
            "BY",
            "ON",
            "AS",
            "WITH",
            "AND",
            "OR",
            "NULL",
            "HAVING",
            "DISTINCT",
            "LIMIT",
            "OFFSET",
            "UNION",
            "INTERSECT",
            "INNER JOIN",
            "LEFT JOIN",
            "FULL JOIN",
            "FULL OUTER JOIN",
            "UNION ALL",
            "RIGHT JOIN",
            "OUTER JOIN",
            "GROUP BY",
            "ORDER BY",
        }

        # Filtering tables_aliases, excluding keywords
        tables = {
            k: v
            for k, v in parser.tables_aliases.items()
            if k.upper() not in sql_keywords
        }

        # Getting all columns for verification
        columns = parser.columns_dict
        all_columns = (
            columns.get("select", [])
            + columns.get("join", [])
            + columns.get("where", [])
        )
        logger.info(f"All columns in the query: {columns}")

        # Filtering tables, excluding columns
        valid_tables = {}
        for table in parser.tables:
            # If the 'table' contains a dot and matches a column, skip it
            if "." in table and table in all_columns:
                continue
            # If the table already exists in tables as a value or is not a keyword
            if (
                table not in tables.values()
                and table not in tables
                and table.upper() not in sql_keywords
            ):
                valid_tables[table] = table
            elif table in tables.values():
                # If it is a value from tables_aliases, save the original pair.
                for alias, full_name in tables.items():
                    if full_name == table:
                        valid_tables[alias] = full_name

        tables.update(valid_tables)
        logger.info(f"Extracted tables and aliases (filtered): {tables}")

        used_columns = set()
        for column_ref in all_columns:
            # Checking if this is a string or a list
            if isinstance(column_ref, list):
                # If it's a list, process each element
                for sub_ref in column_ref:
                    if isinstance(sub_ref, str):
                        parts = sub_ref.split(".")
                        if len(parts) >= 2:
                            table_or_alias = parts[-2].strip()
                            column = parts[-1].strip()
                            full_table = tables.get(table_or_alias, table_or_alias)
                            if (
                                f"{target_schema}.{target_table}" in full_table
                                or target_table == full_table
                                or full_table.endswith(f".{target_table}")
                            ) and column in target_columns:
                                used_columns.add(column)
            elif isinstance(column_ref, str):
                # A regular string
                parts = column_ref.split(".")
                if len(parts) >= 2:
                    table_or_alias = parts[-2].strip()
                    column = parts[-1].strip()
                    full_table = tables.get(table_or_alias, table_or_alias)
                    if (
                        f"{target_schema}.{target_table}" in full_table
                        or target_table == full_table
                        or full_table.endswith(f".{target_table}")
                    ) and column in target_columns:
                        used_columns.add(column)

        # Recursively process nested CTEs
        if parser.with_queries:
            logger.info("Processing CTE")
            for cte_name, cte_query in parser.with_queries.items():
                logger.info(f"CTE {cte_name}")
                cte_used_columns = find_column_usage(
                    cte_query, target_schema, target_table, target_columns
                )

            if cte_used_columns:
                used_columns.update(cte_used_columns)

        logger.info(f"Found used columns: {used_columns}")

        return used_columns
    except Exception as e:
        logger.error(f"SQL parsing error: {e}")
        return set()


def analyze_sql_files(folder_path, target_schema, target_table, target_columns):
    """Main function for analyzing SQL files."""
    sql_file_paths = find_sql_files(folder_path)
    results = {}

    logger.info(f"Found {len(sql_file_paths)} files to analyze")

    for sql_file_path in sql_file_paths:
        sql_content, view_name = get_file_content(sql_file_path)
        if sql_content:
            logger.info(f"File: {sql_file_path}")
            used_columns = find_column_usage(
                sql_content, target_schema, target_table, target_columns
            )
            if used_columns:
                results[sql_file_path] = {
                    "view_name": view_name,
                    "used_columns": list(used_columns),
                }
            logger.info("\n")

    # Вывод результатов
    if results:
        logger.warning("\nColumns' usage found in the following files:")
        for file_path, info in results.items():
            logger.warning(f"File: {file_path}")
            logger.warning(f"View: {info['view_name']}")
            logger.warning(f"Used columns: {', '.join(info['used_columns'])}")
    else:
        logger.warning("The specified columns were not found in any file")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    folder_path = "sql_code"
    target_schema = "public"
    target_table = "table_2"
    target_columns = '["minutes", "fake"]'

    parser.add_argument(
        "--folder_path", type=str, default=folder_path, help="Folder to search SQL code"
    )
    parser.add_argument(
        "--target_schema",
        type=str,
        default=target_schema,
        help="DWH Schema of the target table",
    )
    parser.add_argument(
        "--target_table",
        type=str,
        default=target_table,
        help="The name of the DWH table whose usage we will be searching for",
    )
    parser.add_argument(
        "--target_columns",
        type=str,
        default=target_columns,
        help="The list of columns in the table, the usage of which we will be searching for",
    )

    args = parser.parse_args()

    analyze_sql_files(
        args.folder_path,
        args.target_schema,
        args.target_table,
        json.loads(args.target_columns),
    )

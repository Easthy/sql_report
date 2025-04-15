import sys
import os
import re
import argparse
import csv
import json
import yaml

import psycopg2
from sql_metadata import Parser
import sqlparse
import logging

from rich.logging import RichHandler
from rich.text import Text

logging.basicConfig(
    level=logging.INFO, handlers=[RichHandler(rich_tracebacks=True, show_time=False)]
)
logger = logging.getLogger("rich")


def get_connection():
    return psycopg2.connect(**DATABASE_CONFIG)


def get_table_columns(table_name, db_report_schema):
    query = """
       SELECT column_name
        FROM svv_columns
       WHERE table_schema = %s
             AND table_name = %s
    ORDER BY ordinal_position;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    db_report_schema,
                    table_name,
                ),
            )
            return [row[0] for row in cur.fetchall()]


def get_table_size(table_name, db_report_schema):
    query = f"""
    DROP TABLE IF EXISTS evaluation_tmp
    ;
    CREATE TEMPORARY table evaluation_tmp AS
    SELECT *
      FROM {db_report_schema}.{table_name}
    ;
    ANALYZE evaluation_tmp;
    SELECT size
     FROM svv_table_info
    WHERE "table" = 'evaluation_tmp'
    ;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            return result[0]


def get_table_rows(table_name, db_report_schema):
    query = f"""
    SELECT COUNT(1)
      FROM {db_report_schema}.{table_name}
    ;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            return result[0]


def find_sql_files(folder_path):
    return [
        os.path.join(root, file)
        for root, _, files in os.walk(folder_path)
        for file in files
        if file.endswith(".sql")
    ]


def get_file_content(file_path):
    pattern = r"CREATE OR REPLACE VIEW (\w+\.\w+)"
    with open(file_path, "r") as sql_file:
        first_line = sql_file.readline()
        view_name = (
            re.search(pattern, first_line).group(1)
            if re.search(pattern, first_line)
            else None
        )
        sql_content = "".join(
            line
            for line in sql_file
            if "WITH NO SCHEMA BINDING" not in line
            and "CREATE OR REPLACE VIEW" not in line
        )
    return sql_content, view_name


def count_sql_elements(sql_content):
    lower_sql = sql_content.lower()
    elements = {
        "join_count": lower_sql.count("join"),
        "cross_join_count": lower_sql.count("cross join"),
        "case_count": lower_sql.count("case"),
        "union_count": lower_sql.count("union"),
        "regexp_count": sum(
            lower_sql.count(func)
            for func in [
                "regexp_substr",
                "regexp_replace",
                "regexp_instr",
                "regexp_count",
            ]
        ),
    }
    return elements


def count_sql_operators(sql_content):
    operators = [
        "json_extract_path",
        "nvl",
        "coalesce",
        "group by",
        "order by",
        "having",
        "distinct",
        "listagg",
        "split_part",
        "substring",
        "over",
        "date_trunc",
        "date_part",
        "json_parse",
        "json_serialize",
    ]
    return sum(sql_content.lower().count(op.lower()) for op in operators)


def to_csv(json_data, output_file="output.csv"):
    headers = json_data[0].keys()
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter="|")
        writer.writeheader()
        writer.writerows(json_data)
        logger.warning(f"Report was saved into {output_file}")


def analyze_sql_files(file_path, db_search, db_report_schema):
    sql_file_paths = find_sql_files(file_path)
    logger.info(f"Found {len(sql_file_paths)} files to analyze")
    stats = []
    for sql_file_path in sql_file_paths:
        logger.info(f"Working on the file: {sql_file_path}")

        sql_content, view_name = get_file_content(sql_file_path)
        sql_parsed_lib_sql_parse = sqlparse.parse(sql_content)[0]
        sql_parsed_lib_sql_metadata = Parser(sql_content)

        tables_used = [
            table
            for table in sql_parsed_lib_sql_metadata.tables
            if table.lower()
            not in (
                "current_date",
                "date_trunc",
                "current_timestamp",
                "case",
                "lower",
                "nvl",
                "count",
                "sum",
                "position",
            )
        ]
        cte_used = sql_parsed_lib_sql_metadata.with_names
        subqueries_used = sql_parsed_lib_sql_metadata.subqueries_names

        sql_elements = count_sql_elements(sql_parsed_lib_sql_parse.value)
        sql_operators = count_sql_operators(sql_parsed_lib_sql_parse.value)
        columns = get_table_columns(view_name.split(".")[1], db_report_schema)

        data = {
            "view_name": view_name,
            "sql_file_path": sql_file_path,
            "score": (len(tables_used) * 0.2)
            + (sql_operators * 0.1)
            + (sql_elements.get("join_count") * 0.3)
            + (len(subqueries_used) * 0.5)
            + (len(cte_used) * 0.5)
            + (sql_elements.get("case_count") * 0.2)
            + (sql_elements.get("union_count") * 0.4)
            + (sql_elements.get("cross_join_count") * 0.7)
            + (sql_elements.get("regexp_count") * 0.6),
            "tables_used_cnt": len(tables_used),
            "columns_cnt": len(columns),
            "sql_operators_cnt": sql_operators,
            "join_cnt": sql_elements.get("join_count"),
            "subqueries_used_cnt": len(subqueries_used),
            "cte_used_cnt": len(cte_used),
            "case_cnt": sql_elements.get("case_count"),
            "union_cnt": sql_elements.get("union_count"),
            "cross_join_cnt": sql_elements.get("cross_join_count"),
            "regexp_cnt": sql_elements.get("regexp_count"),
            "columns": columns,
            "tables_used": tables_used,
            "cte_used": cte_used,
            "subqueries_used": subqueries_used,
        }

        if db_search:
            data["size_mb"] = get_table_size(view_name.split(".")[1], db_report_schema)
            data["rows_cnt"] = get_table_rows(view_name.split(".")[1], db_report_schema)

        stats.append(data)

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file_path", type=str, default="sql_code", help="Folder to search SQL code"
    )
    parser.add_argument(
        "--db_config",
        type=str,
        default="db_config.yml",
        help="DB connection config file",
    )
    parser.add_argument(
        "--db_search",
        type=bool,
        default=False,
        help="Search and analyze view at your DWH",
    )
    parser.add_argument(
        "--db_report_schema",
        type=str,
        default="public",
        help="DWH schema of the report view",
    )

    args = parser.parse_args()

    with open(args.db_config, "r") as file:
        DATABASE_CONFIG = yaml.safe_load(file)

    stats = analyze_sql_files(args.file_path, args.db_search, args.db_report_schema)
    to_csv(stats)

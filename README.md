# The purpose of this code is to find in which script the columns of a given table are used (find_usages). Properly formatted SQL code is required for parsing in this case; otherwise, errors related to the parsing library may occur.
Additionally, the code allows for evaluating the complexity of the scripts and identifying potential bottlenecks that may require refactoring.

## To run the script that will evaluate the complexity of the code.
```
python3 evaluate_complicity.py --help
python3 evaluate_complicity.py --db_search=True --db_report_schema=public
```
The results of the script evaluations will be in the file `output.csv`.  
To speed up execution, you can disable `get_table_size` and `get_table_rows` in the evaluation script by setting db_search to False

## To run the script that finds in which SQL query files columns from a given table are used.
```
python3 find_usage.py --help
python3 find_usage.py --target_columns='["minutes", "fake"]'
```
![alt text](https://github.com/Easthy/sql_report/blob/main/screenshot.png)

### SQL Rules

1. **An alias for each column must be specified..**  
2. **Columns and tables must have different names (aliases).**  
3. **All aliases must be unique in the code. There should not be two identical aliases.**
4. **An alias for a column and a table is specified using the keyword `AS`.**  
5. **Aliases must not be shorter than 4 characters.**  
6. **Avoid multiple renaming. If a calculation has already been assigned an alias, do not give it a new one.**
```
WITH table_1 AS (
    SELECT table.col
      FROM table
)
SELECT *
  FROM table_2

       LEFT JOIN table_1 AS tbl_1
       ON tbl_1.col = table_2.col
```
7. **Using `*` in SELECT is prohibited. Explicitly list the in `SELECT`.**
8. **After the last `JOIN` that uses a column from a subquery, and if there are subsequent SQL operators, there must be a character (e.g., a space) following it.**
```
  SELECT table.col_1 
    FROM (
          SELECT table.project_id,
                 table.col_1
            FROM table
         ) AS table
         
         LEFT JOIN public.projects
         ON projects.project_id = table.project_id
/* Here, at the beginning of the line, there must be a space character */
GROUP BY table.col_1 
```
   - Otherwise, an error will occur:  
     ```
     SQL parsing error: 'project_idGROUP' is not in list.
     ```  
9. **After the expressions `UNION`, `UNION ALL`, and before the next SQL operator, there must be a character (e.g., a space) following it.**
```
SELECT table.col
  FROM table

UNION ALL
/* Here, at the beginning of the line, there must be a space character */
SELECT table_2.col
  FROM table_2
```

## Recommendations for Writing SQL
### https://www.sqlstyle.guide/

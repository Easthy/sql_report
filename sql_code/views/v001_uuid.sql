CREATE OR REPLACE VIEW reports.v001_uuid AS

WITH cte_data AS (
     WITH sub_cte_data AS (
            SELECT table_2.user_id,
                   SUM(table_2.minutes) AS minutes
              FROM public.table_2
             WHERE table_2.colum_1 NOT IN (44, 55, 61, 72, 84, 101, 131)
          GROUP BY table_2.user_id
     )
     SELECT sub_cte_data.user_id,
            sub_cte_data.minutes
       FROM sub_cte_data
)
SELECT table.uuid,
       table.user_id,
       table.registration_date,
       table.registration_ip,
       table.last_ip,
       subquery.minutes
  FROM public.table
       
       LEFT JOIN (
          SELECT cte_data.user_id,
                 cte_data.minutes
            FROM cte_data
       ) AS subquery
       ON subquery.user_id = table.user_id
 WHERE table.fake IS NOT TRUE

UNION ALL

SELECT NULL AS uuid,
       table_3.user_id,
       COALESCE(table_3.registration_date, CURRENT_DATE) AS registration_date,
       table_3.registration_ip,
       table_3.last_ip,
       CASE
          WHEN table_3.minutes < 5 
               THEN 0
          ELSE table_3.minutes
       END AS minutes
  FROM public.table_3

WITH NO SCHEMA BINDING
;


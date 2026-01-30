-- run_id_inventory.sql
-- Operator helper: inventory all columns whose name contains RUN_ID in database MIP.
-- Use to verify RUN_ID hygiene (all should be VARCHAR/STRING, no numeric coercion).

use database MIP;

select
    table_catalog,
    table_schema,
    table_name,
    column_name,
    data_type
from MIP.INFORMATION_SCHEMA.COLUMNS
where upper(column_name) like '%RUN_ID%'
order by table_schema, table_name, column_name;

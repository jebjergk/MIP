-- discover_trust_objects.sql
-- Purpose: Locate trust/gating related objects in APP/MART

use role MIP_ADMIN_ROLE;
use database MIP;

with trust_terms as (
    select column1 as term
    from values
        ('%TRUST%'),
        ('%GATE%'),
        ('%LABEL%'),
        ('%ACTION%')
),
views_matches as (
    select
        'VIEW' as OBJECT_TYPE,
        v.TABLE_SCHEMA as SCHEMA_NAME,
        v.TABLE_NAME as OBJECT_NAME,
        v.COMMENT as OBJECT_COMMENT
    from MIP.INFORMATION_SCHEMA.VIEWS v
    where v.TABLE_SCHEMA in ('APP', 'MART')
      and exists (
          select 1
          from trust_terms t
          where v.TABLE_NAME ilike t.term
             or v.VIEW_DEFINITION ilike t.term
      )
),
table_matches as (
    select
        'TABLE' as OBJECT_TYPE,
        t.TABLE_SCHEMA as SCHEMA_NAME,
        t.TABLE_NAME as OBJECT_NAME,
        t.COMMENT as OBJECT_COMMENT
    from MIP.INFORMATION_SCHEMA.TABLES t
    where t.TABLE_SCHEMA in ('APP', 'MART')
      and exists (
          select 1
          from trust_terms tt
          where t.TABLE_NAME ilike tt.term
      )
),
procedure_matches as (
    select
        'PROCEDURE' as OBJECT_TYPE,
        p.PROCEDURE_SCHEMA as SCHEMA_NAME,
        p.PROCEDURE_NAME as OBJECT_NAME,
        p.COMMENT as OBJECT_COMMENT
    from MIP.INFORMATION_SCHEMA.PROCEDURES p
    where p.PROCEDURE_SCHEMA in ('APP', 'MART')
      and exists (
          select 1
          from trust_terms tt
          where p.PROCEDURE_NAME ilike tt.term
             or p.ARGUMENT_SIGNATURE ilike tt.term
      )
)
select * from views_matches
union all
select * from table_matches
union all
select * from procedure_matches
order by OBJECT_TYPE, SCHEMA_NAME, OBJECT_NAME;

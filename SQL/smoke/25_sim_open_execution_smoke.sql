-- 25_sim_open_execution_smoke.sql
-- Verifies simulation open executor wiring and decoupled daily pipeline behavior.

select
  'SIM_OPEN_EXECUTOR_EXISTS' as check_name,
  count(*) as cnt
from MIP.INFORMATION_SCHEMA.PROCEDURES
where PROCEDURE_SCHEMA = 'APP'
  and PROCEDURE_NAME = 'SP_RUN_SIM_OPEN_EXECUTION';

-- Run executor for one portfolio (safe no-op when no proposed run exists).
call MIP.APP.SP_RUN_SIM_OPEN_EXECUTION(1, null);

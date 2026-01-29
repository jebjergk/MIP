# Snowflake SQL Limitations (MIP Conventions)

Notes for future development and AI agents: avoid known Snowflake compilation/context issues when writing stored procedures.

---

## SELECT ... INTO in Stored Procedures

**Problem:** In Snowflake SQL stored procedures, `SELECT ... INTO :variable FROM (...)` often fails with:

- `syntax error ... unexpected 'into'`
- `INTO clause is not allowed in this context`

This can occur when the `SELECT` uses subqueries, CTEs (`WITH`), or certain `FROM` forms. The compiler disallows `INTO` in those contexts.

**Preferred patterns:**

1. **Scalar / single value:** Use assignment with a scalar subquery instead of `INTO`:
   ```sql
   -- Avoid:
   select col into :v_var from t where ... limit 1;

   -- Use:
   v_var := (select col from t where ... limit 1);
   ```

2. **Multiple columns from one row:** Either assign each column with a separate subquery, or use a RESULTSET + FOR loop:
   ```sql
   -- Option A: separate assigns
   v_a := (select col_a from t where ... limit 1);
   v_b := (select col_b from t where ... limit 1);

   -- Option B: RESULTSET + FOR loop
   v_rs resultset;
   v_rs := (select col_a, col_b from t where ... limit 1);
   for rec in v_rs do
     v_a := rec.col_a;
     v_b := rec.col_b;
     break;
   end for;
   ```

3. **Aggregates / complex queries (e.g. ARRAY_AGG from subquery):** Use RESULTSET + FOR loop; do **not** use `SELECT array_agg(...) INTO :v_var FROM (subquery)`:
   ```sql
   v_rs resultset;
   v_rs := (
     select array_agg(obj) within group (order by rn) as agg
     from ( ... subquery ... ) sub
   );
   for rec in v_rs do
     v_var := rec.agg;
     break;
   end for;
   ```

**Summary:** In MIP stored procedures, avoid `SELECT ... INTO` when the query involves subqueries or CTEs. Use `variable := (SELECT ...)` for scalars, and RESULTSET + `FOR rec IN resultset DO ... END FOR` for single-row or aggregate results from complex queries.

---

*See `MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF` (193_sp_agent_generate_morning_brief.sql) for a full example using these patterns.*

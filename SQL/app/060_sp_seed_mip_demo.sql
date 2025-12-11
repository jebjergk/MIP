use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_SEED_MIP_DEMO()
returns string
language sql
as
$$
begin
    merge into MIP.APP.PATTERN_DEFINITION t
    using (
        select
            'MOMENTUM_DEMO'              as NAME,
            'Demo pattern for stock momentum' as DESCRIPTION,
            object_construct(
                'fast_window', 20,
                'slow_window', 3,
                'lookback_days', 1,
                'min_return', 0.002,
                'min_zscore', 1.0,
                'market_type', 'STOCK',
                'interval_minutes', 5
            ) as PARAMS_JSON
    ) s
       on t.NAME = s.NAME
     when not matched then
        insert (NAME, DESCRIPTION, PARAMS_JSON, IS_ACTIVE, ENABLED)
        values (s.NAME, s.DESCRIPTION, s.PARAMS_JSON, 'Y', true)
     when matched then
        update set
            t.DESCRIPTION = s.DESCRIPTION,
            t.PARAMS_JSON = coalesce(t.PARAMS_JSON, s.PARAMS_JSON),
            t.IS_ACTIVE   = coalesce(t.IS_ACTIVE, 'Y'),
            t.ENABLED     = coalesce(t.ENABLED, true);

    return 'Seeded MOMENTUM_DEMO pattern (upserted without truncation).';
end;
$$;

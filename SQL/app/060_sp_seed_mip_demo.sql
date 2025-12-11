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
            'MOMENTUM_DEMO'                    as NAME,
            'Demo pattern for stock momentum'  as DESCRIPTION,
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
       on upper(t.NAME) = upper(s.NAME)
     when matched then
        update set
            t.DESCRIPTION = s.DESCRIPTION,
            t.PARAMS_JSON = s.PARAMS_JSON,
            t.IS_ACTIVE   = 'Y',
            t.ENABLED     = true,
            t.UPDATED_AT  = current_timestamp(),
            t.UPDATED_BY  = current_user()
     when not matched then
        insert (NAME, DESCRIPTION, PARAMS_JSON, IS_ACTIVE, ENABLED)
        values (s.NAME, s.DESCRIPTION, s.PARAMS_JSON, 'Y', true);

    -- Seed minimal demo bars with a dedicated symbol so this procedure remains non-destructive.
    merge into MIP.RAW_EXT.MARKET_BARS_RAW t
    using (
        select
            to_timestamp_ntz('2024-05-01 09:30:00') as TS,
            'MIP_DEMO'                              as SYMBOL,
            'DEMO'                                  as SOURCE,
            'STOCK'                                 as MARKET_TYPE,
            5                                       as INTERVAL_MINUTES,
            100.00                                  as OPEN,
            101.25                                  as HIGH,
            99.75                                   as LOW,
            101.00                                  as CLOSE,
            150000                                  as VOLUME,
            object_construct('seed', 'MOMENTUM_DEMO') as RAW
        union all
        select
            to_timestamp_ntz('2024-05-01 09:35:00'),
            'MIP_DEMO',
            'DEMO',
            'STOCK',
            5,
            101.00,
            102.50,
            100.90,
            102.25,
            162500,
            object_construct('seed', 'MOMENTUM_DEMO')
        union all
        select
            to_timestamp_ntz('2024-05-01 09:40:00'),
            'MIP_DEMO',
            'DEMO',
            'STOCK',
            5,
            102.25,
            102.75,
            101.50,
            102.10,
            158000,
            object_construct('seed', 'MOMENTUM_DEMO')
    ) s
       on t.TS = s.TS
      and t.SYMBOL = s.SYMBOL
      and t.MARKET_TYPE = s.MARKET_TYPE
      and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
     when matched then
        update set
            t.SOURCE          = s.SOURCE,
            t.OPEN            = s.OPEN,
            t.HIGH            = s.HIGH,
            t.LOW             = s.LOW,
            t.CLOSE           = s.CLOSE,
            t.VOLUME          = s.VOLUME,
            t.RAW             = s.RAW,
            t.INGESTED_AT     = coalesce(t.INGESTED_AT, current_timestamp())
     when not matched then
        insert (TS, SYMBOL, SOURCE, MARKET_TYPE, INTERVAL_MINUTES, OPEN, HIGH, LOW, CLOSE, VOLUME, RAW, INGESTED_AT)
        values (s.TS, s.SYMBOL, s.SOURCE, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME, s.RAW, current_timestamp());

    return 'Seeded MOMENTUM_DEMO pattern and demo bars (idempotent, non-destructive).';
end;
$$;

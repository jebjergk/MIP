use role MIP_ADMIN_ROLE;
use database MIP;

begin
    for d_rec in (
        select distinct TS::date as AS_OF_DATE
        from MIP.APP.PORTFOLIO_DAILY
        where TS::date between '2026-02-03'::date and '2026-03-06'::date
        order by AS_OF_DATE
    ) do
        call MIP.APP.SP_RUN_PARALLEL_WORLDS(
            'BACKFILL_PW_FIX_DFLT_' || to_varchar(d_rec.AS_OF_DATE, 'YYYYMMDD'),
            d_rec.AS_OF_DATE::timestamp_ntz,
            null,
            'DEFAULT_ACTIVE'
        );

        call MIP.APP.SP_RUN_PARALLEL_WORLDS(
            'BACKFILL_PW_FIX_SWEEP_' || to_varchar(d_rec.AS_OF_DATE, 'YYYYMMDD'),
            d_rec.AS_OF_DATE::timestamp_ntz,
            null,
            'SWEEP'
        );
    end for;

    return 'BACKFILL_DONE';
end;

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
            'Demo pattern for stock momentum' as DESCRIPTION
    ) s
       on t.NAME = s.NAME
     when not matched then
        insert (NAME, DESCRIPTION, IS_ACTIVE, ENABLED)
        values (s.NAME, s.DESCRIPTION, 'Y', true)
     when matched then
        update set
            t.DESCRIPTION = s.DESCRIPTION,
            t.IS_ACTIVE   = coalesce(t.IS_ACTIVE, 'Y'),
            t.ENABLED     = coalesce(t.ENABLED, true);

    return 'Seeded MOMENTUM_DEMO pattern (upserted without truncation).';
end;
$$;

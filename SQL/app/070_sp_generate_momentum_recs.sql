use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_SEED_MIP_DEMO()
returns varchar
language sql
as
$$
declare
    v_pattern_id number;
begin
    -- Try to fetch existing pattern
    select PATTERN_ID
      into :v_pattern_id
    from MIP.APP.PATTERN_DEFINITION
    where NAME = 'MOMENTUM_DEMO'
    limit 1;

    if (v_pattern_id is null) then
        -- Pattern does not exist → create it
        insert into MIP.APP.PATTERN_DEFINITION (NAME, DESCRIPTION, ENABLED)
        values (
            'MOMENTUM_DEMO',
            'Demo pattern: flags bars with positive simple return above a threshold.',
            true
        );

        return 'MOMENTUM_DEMO pattern created.';
    else
        -- Pattern exists → update fields
        update MIP.APP.PATTERN_DEFINITION
        set ENABLED     = true,
            DESCRIPTION = 'Demo pattern: flags bars with positive simple return above a threshold.',
            UPDATED_AT  = current_timestamp(),
            UPDATED_BY  = current_user()
        where PATTERN_ID = :v_pattern_id;

        return 'MOMENTUM_DEMO pattern already existed, updated.';
    end if;
end;
$$;

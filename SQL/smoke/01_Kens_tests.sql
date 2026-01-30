use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;
call MIP.APP.SP_RUN_DAILY_PIPELINE();

SET test_run_id = uuid_string();
SET portfolio_id = 1;

CALL MIP.APP.SP_WRITE_MORNING_BRIEF(
    $portfolio_id,
    (SELECT max(ts)::timestamp_ntz FROM MIP.MART.MARKET_BARS WHERE interval_minutes = 1440),
    $test_run_id
);

EXECUTE IMMEDIATE $$
DECLARE
    test_run_id   VARCHAR DEFAULT uuid_string();
    as_of_ts      TIMESTAMP_NTZ DEFAULT (SELECT max(ts)::timestamp_ntz FROM MIP.MART.MARKET_BARS WHERE interval_minutes = 1440);
    portfolio_id  NUMBER DEFAULT 1;
BEGIN
    CALL MIP.APP.SP_WRITE_MORNING_BRIEF(:portfolio_id, :as_of_ts, :test_run_id);
END;
$$;
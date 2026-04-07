-- Smoke: bracket realism APP_CONFIG keys exist after 495 script
select 'LIVE_BRACKET_REALISM_ENABLED' as k,
       (select count(*) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'LIVE_BRACKET_REALISM_ENABLED') as present
union all
select 'LIVE_BRACKET_MIN_GROSS_TP_PCT_OF_NOTIONAL',
       (select count(*) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'LIVE_BRACKET_MIN_GROSS_TP_PCT_OF_NOTIONAL')
union all
select 'LIVE_MIN_BRACKET_WIDTH_BPS',
       (select count(*) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'LIVE_MIN_BRACKET_WIDTH_BPS');

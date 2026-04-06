use role MIP_ADMIN_ROLE;
use database MIP;
use schema APP;

call MIP.APP.SP_RUN_IB_SYMBOL_ONBOARDING(
    parse_json('["MRK","ABBV","COP","NEE","QCOM","ORCL","PANW","SBUX","MCD","TGT","DAL","NUE","SLB","FCX","NET","CRWD"]'),
    'STOCK',
    '2025-08-01'::date,
    current_date(),
    true,
    null,
    'SUPPLY_EXP_2026',
    55
);

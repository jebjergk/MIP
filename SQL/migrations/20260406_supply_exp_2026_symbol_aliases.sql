-- 20260406_supply_exp_2026_symbol_aliases.sql
-- COMPANY_NAME aliases for SUPPLY_EXP_2026 universe (+ optional-later symbols for when onboarded).

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.NEWS.SYMBOL_ALIAS_DICT t
using (
    select column1 as SYMBOL, 'STOCK' as MARKET_TYPE, column2 as ALIAS, 'COMPANY_NAME' as ALIAS_TYPE, true as IS_ACTIVE
    from values
        ('MRK',  'MERCK'),
        ('ABBV', 'ABBVIE'),
        ('COP',  'CONOCOPHILLIPS'),
        ('NEE',  'NEXTERA ENERGY'),
        ('QCOM', 'QUALCOMM'),
        ('ORCL', 'ORACLE'),
        ('PANW', 'PALO ALTO NETWORKS'),
        ('SBUX', 'STARBUCKS'),
        ('MCD',  'MCDONALDS'),
        ('TGT',  'TARGET'),
        ('DAL',  'DELTA AIR LINES'),
        ('NUE',  'NUCOR'),
        ('SLB',  'SLB'),
        ('FCX',  'FREEPORT-MCMORAN'),
        ('NET',  'CLOUDFLARE'),
        ('CRWD', 'CROWDSTRIKE'),
        ('LUV',  'SOUTHWEST AIRLINES'),
        ('FSLR', 'FIRST SOLAR'),
        ('MRNA', 'MODERNA'),
        ('DE',   'DEERE')
) s
on t.SYMBOL = s.SYMBOL
and t.MARKET_TYPE = s.MARKET_TYPE
and t.ALIAS = s.ALIAS
when matched then update set
    t.ALIAS_TYPE = s.ALIAS_TYPE,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    SYMBOL, MARKET_TYPE, ALIAS, ALIAS_TYPE, IS_ACTIVE, CREATED_AT, UPDATED_AT
) values (
    s.SYMBOL, s.MARKET_TYPE, s.ALIAS, s.ALIAS_TYPE, s.IS_ACTIVE, current_timestamp(), current_timestamp()
);

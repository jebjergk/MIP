-- 495_live_bracket_realism_app_config.sql
-- Portfolio-relative live bracket realism (API: live.py). Insert-only for missing keys.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select 'LIVE_BRACKET_REALISM_ENABLED' as CONFIG_KEY,
           'true' as CONFIG_VALUE,
           'When false, bracket realism gate is disabled.' as DESCRIPTION
    union all
    select 'LIVE_BRACKET_REALISM_MODE', 'BLOCK', 'OFF WARN BLOCK. WARN is non-enforcing until implemented.'
    union all
    select 'LIVE_BRACKET_REALISM_APPLY_TO_PAPER', 'false', 'When true, run bracket realism when ADAPTER_MODE is PAPER.'
    union all
    select 'LIVE_BRACKET_ABS_MIN_GROSS_TP_USD', '1', 'Layer A: minimum gross take-profit USD.'
    union all
    select 'LIVE_BRACKET_ABS_MIN_GROSS_SL_USD', '1', 'Layer A: minimum gross stop-risk USD.'
    union all
    select 'LIVE_BRACKET_MIN_GROSS_TP_PCT_OF_NOTIONAL', '0.015', 'Layer B: min gross TP as fraction of entry notional (before small-line mult).'
    union all
    select 'LIVE_BRACKET_MIN_NET_TP_PCT_OF_NOTIONAL', '0.01', 'Layer B: min net TP $ vs notional (after fee floor in code).'
    union all
    select 'LIVE_BRACKET_MIN_GROSS_SL_PCT_OF_NOTIONAL', '0.01', 'Layer B: min gross SL $ vs notional.'
    union all
    select 'LIVE_BRACKET_MIN_GROSS_TP_BPS_OF_NAV', '12', 'Layer B NAV bps leg for gross TP, capped by cap mult times pct times N.'
    union all
    select 'LIVE_BRACKET_NAV_RULE_CAP_MULT', '5', 'Caps NAV bps leg vs notional-based leg.'
    union all
    select 'LIVE_BRACKET_SMALL_POS_MAX_PCT_NAV', '0.10', 'If notional/NAV below this, apply STRICT_MULT to Layer B pct requirements.'
    union all
    select 'LIVE_BRACKET_SMALL_POS_STRICT_MULT', '1.2', 'Multiplier on Layer B pct mins when line is small vs NAV.'
    union all
    select 'LIVE_MIN_BRACKET_WIDTH_BPS', '25', 'Minimum min(TP move bps, SL move bps) from entry.'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

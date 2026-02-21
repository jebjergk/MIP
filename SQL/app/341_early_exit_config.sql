-- 341_early_exit_config.sql
-- Purpose: Feature flags and tunable parameters for the early-exit layer.
-- Default: SHADOW mode, OFF globally. Enable per-portfolio via EARLY_EXIT_PORTFOLIOS.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select column1 as CONFIG_KEY, column2 as CONFIG_VALUE, column3 as DESCRIPTION
    from values
        ('EARLY_EXIT_ENABLED',          'false',
         'Master kill switch for early-exit evaluation of daily positions'),

        ('EARLY_EXIT_MODE',             'SHADOW',
         'Execution mode: SHADOW (log only), PAPER (apply to sim), ACTIVE (live)'),

        ('EARLY_EXIT_PAYOFF_MULTIPLIER','1.0',
         'Multiplier on target return for Stage A payoff threshold (1.0 = exact target, 1.2 = 20% buffer)'),

        ('EARLY_EXIT_GIVEBACK_PCT',     '0.40',
         'Fraction of peak return that must be given back to trigger Stage B (0.40 = 40% giveback)'),

        ('EARLY_EXIT_NO_NEW_HIGH_BARS', '3',
         'Number of consecutive 15-min bars with no new high after payoff to confirm giveback'),

        ('EARLY_EXIT_QUICK_PAYOFF_MINS','60',
         'If payoff achieved within this many minutes of entry, treat as higher giveback risk (lower giveback_pct threshold)'),

        ('EARLY_EXIT_QUICK_GIVEBACK_PCT','0.25',
         'Giveback threshold for quick-payoff trades (stricter than normal)'),

        ('EARLY_EXIT_INTERVAL_MINUTES', '15',
         'Bar interval used for early-exit evaluation (must match ingest)'),

        ('EARLY_EXIT_PORTFOLIOS',       'ALL',
         'Comma-separated portfolio IDs to evaluate, or ALL for all active portfolios'),

        ('EARLY_EXIT_MARKET_TYPES',     'STOCK,FX,ETF',
         'Comma-separated market types to evaluate for early exit')
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
    values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);

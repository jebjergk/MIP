-- 372_news_config.sql
-- Purpose: Phase 1 config flags for News Context.
-- Defaults are conservative and display-only.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select column1 as CONFIG_KEY, column2 as CONFIG_VALUE, column3 as DESCRIPTION
    from values
        ('NEWS_ENABLED', 'true',
         'Master feature flag for News Context ingestion and decision-time joins'),
        ('NEWS_SOURCES',
         '["SEC_XBRL_ALL","GLOBENEWSWIRE_RSS","MARKETWATCH_RSS","FED_RSS","ECB_RSS","SEC_RSS_INDEX"]',
         'Approved SOURCE_ID list used by news ingestion'),
        ('NEWS_MATCH_CONFIDENCE_MIN', '0.70',
         'Deterministic minimum confidence threshold for NEWS_SYMBOL_MAP inclusion'),
        ('NEWS_BURST_Z_HOT', '1.50',
         'Z-score threshold for HOT news context badge'),
        ('NEWS_DISPLAY_ONLY', 'false',
         'When true, news context is exposed for explainability only with no behavior change'),
        ('NEWS_MODULATION_ENABLED', 'false',
         'When true, bounded non-directional decision modulation can be applied'),
        ('NEWS_INFLUENCE_ENABLED', 'true',
         'Master switch for proposal scoring influence from news context'),
        ('NEWS_DECAY_TAU_HOURS', '24',
         'Time-decay half-life control for proposal-time news impact weighting'),
        ('NEWS_PRESSURE_HOT', '0.12',
         'Positive score lift weight for HOT/WARM contextual pressure'),
        ('NEWS_UNCERTAINTY_HIGH', '0.08',
         'Penalty weight applied when uncertainty_flag is true'),
        ('NEWS_EVENT_RISK_HIGH', '0.10',
         'Penalty weight applied for high event-risk proxy'),
        ('NEWS_SCORE_MAX_ABS', '0.20',
         'Absolute cap on per-proposal news score adjustment'),
        ('NEWS_STALENESS_THRESHOLD_MINUTES', '180',
         'Threshold for marking decision-time news context as stale'),
        ('NEWS_RETENTION_DAYS_HOT', '90',
         'Hot retention window in days for NEWS schema tables'),
        ('NEWS_RETENTION_DAYS_ARCHIVE', '365',
         'Archive retention target for older news records')
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

select
    CONFIG_KEY,
    CONFIG_VALUE,
    DESCRIPTION
from MIP.APP.APP_CONFIG
where CONFIG_KEY like 'NEWS_%'
order by CONFIG_KEY;

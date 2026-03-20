use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.GLOSSARY_TERM t
using (
  select
    'nav' as TERM_KEY,
    'NAV' as DISPLAY_TERM,
    parse_json('["net asset value","equity","portfolio value"]') as ALIASES,
    'portfolio' as CATEGORY,
    'NAV means Net Asset Value: the total portfolio value (cash plus positions) at a point in time.' as DEFINITION_SHORT,
    'In MIP screens, NAV is the broker-truth portfolio value snapshot. It is used to track level and change over time.' as DEFINITION_LONG,
    'On Live Portfolio Activity, NAV reflects the linked IBKR account value shown in Snapshot and trend charts.' as MIP_SPECIFIC_MEANING,
    'In finance, NAV is the per-fund or per-portfolio asset value after liabilities.' as GENERAL_MARKET_MEANING,
    'Live Portfolio Activity top cards and Snapshot Trends chart.' as EXAMPLE_IN_MIP,
    parse_json('["equity","cash","unrealized p&l","drawdown"]') as RELATED_TERMS,
    'SEED' as SOURCE_TYPE,
    '412_ask_mip_nav_glossary_seed.sql' as SOURCE_REF,
    true as IS_APPROVED,
    'approved' as REVIEW_STATUS
) s
on t.TERM_KEY = s.TERM_KEY
when matched then update set
  DISPLAY_TERM = s.DISPLAY_TERM,
  ALIASES = s.ALIASES,
  CATEGORY = s.CATEGORY,
  DEFINITION_SHORT = s.DEFINITION_SHORT,
  DEFINITION_LONG = s.DEFINITION_LONG,
  MIP_SPECIFIC_MEANING = s.MIP_SPECIFIC_MEANING,
  GENERAL_MARKET_MEANING = s.GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP = s.EXAMPLE_IN_MIP,
  RELATED_TERMS = s.RELATED_TERMS,
  SOURCE_TYPE = s.SOURCE_TYPE,
  SOURCE_REF = s.SOURCE_REF,
  IS_APPROVED = s.IS_APPROVED,
  REVIEW_STATUS = s.REVIEW_STATUS,
  UPDATED_AT = current_timestamp()
when not matched then insert (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY, DEFINITION_SHORT, DEFINITION_LONG,
  MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING, EXAMPLE_IN_MIP, RELATED_TERMS,
  SOURCE_TYPE, SOURCE_REF, IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
)
values (
  s.TERM_KEY, s.DISPLAY_TERM, s.ALIASES, s.CATEGORY, s.DEFINITION_SHORT, s.DEFINITION_LONG,
  s.MIP_SPECIFIC_MEANING, s.GENERAL_MARKET_MEANING, s.EXAMPLE_IN_MIP, s.RELATED_TERMS,
  s.SOURCE_TYPE, s.SOURCE_REF, s.IS_APPROVED, s.REVIEW_STATUS, current_timestamp(), current_timestamp()
);

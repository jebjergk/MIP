use role MIP_ADMIN_ROLE;
use database MIP;

select count(*) as glossary_term_count
from MIP.APP.GLOSSARY_TERM;

select count(*) as approved_seed_terms
from MIP.APP.GLOSSARY_TERM
where IS_APPROVED = true
  and TERM_KEY in ('trusted','watch','confidence','max hold','drawdown','catalyst');

insert into MIP.AGENT_OUT.ASK_QUERY_EVENT (
  QUESTION, ROUTE, INTENT, NORMALIZED_TERMS, MATCHED_SOURCE_TYPES,
  DOCS_CONFIDENCE, GLOSSARY_CONFIDENCE, WEB_CONFIDENCE, OVERALL_CONFIDENCE,
  WEB_FALLBACK_USED, ANSWER_FAILED, SUGGESTED_TERMS, UNKNOWN_TERMS
)
select
  'smoke: what is drawdown',
  '/training',
  'term_definition',
  parse_json('["drawdown"]'),
  parse_json('["GLOSSARY"]'),
  0.20, 0.92, 0.00, 0.71,
  false, false,
  parse_json('["drawdown"]'),
  parse_json('[]');

select * from MIP.MART.V_ASK_COVERAGE_METRICS order by DAY desc limit 3;
select * from MIP.MART.V_ASK_UNKNOWN_TERMS order by ASK_COUNT desc limit 10;

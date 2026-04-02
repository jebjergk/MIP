-- 20_entry_intel_phase3_committee_audit.sql
-- After at least one committee completion post–Phase 3 deploy, expect VERDICT_JSON keys.
-- Run as MIP_ADMIN_ROLE. Empty result set is OK if no committee rows yet.

use role MIP_ADMIN_ROLE;
use database MIP;

select
    'PHASE3_VERDICT_SHAPE' as check_label,
    v.ACTION_ID,
    v.VERDICT_JSON:alpha_override_class::string as alpha_override_class,
    v.VERDICT_JSON:entry_intel_audit_v1:comparison_rule_version::string as audit_rule,
    v.VERDICT_JSON:entry_intel_audit_v1:alpha_deviation_justification_status::string as just_status
from MIP.LIVE.COMMITTEE_VERDICT v
order by v.CREATED_AT desc
limit 5;

/* ================================================================
   569_phase4_get_dossier_slice.sql
   Phase 4 Cortex Agentic Proposal Board: GET_PHASE4_DOSSIER_SLICE
   Snowpark stored procedure used as the `generic` tool backing
   for all PHASE4_*_AGENT objects.

   Mirrors GET_SHADOW_EVIDENCE_SLICE shape and guard pattern.

   ALLOWED ROLES:
     MARKET_STRUCTURE,
     LEVEL_PRICE_ACTION,
     THESIS,
     HISTORICAL_EVIDENCE,
     RISK_EXECUTION,
     CHAIR

   ALLOWED SLICES (full catalog):
     identity                 -> symbol/market_type/as_of_date/portfolio
     price                    -> current price + source + recent close ref
     recent_bars              -> compact recent bar array
     candle_sequence          -> compact recent candle pattern descriptors
     recent_price_action      -> short text summary
     levels                   -> nearest support/resistance + significance + distance
     structure                -> structural state + state_confidence
     regime                   -> trend/vol/range regime tags
     long_pattern_signs       -> array of long-side pattern evidence
     short_pattern_signs      -> array of short-side pattern evidence
     setup_events             -> evidence-only setup events (no direction inheritance)
     invalidation_evidence    -> recent invalidation observations
     history                  -> long_history + short_history arrays
     memory                   -> recent_trade_memory + recent_proposal_memory + open_position_context
     policy_flags             -> short_research_visible / short_live_enabled / fx_live_enabled / warnings

   Role-to-slice access map (closed world):
     MARKET_STRUCTURE     -> identity, price, recent_bars, candle_sequence,
                              recent_price_action, structure, regime
     LEVEL_PRICE_ACTION   -> identity, price, recent_bars, candle_sequence,
                              recent_price_action, levels
     THESIS               -> identity, price, structure, regime, levels,
                              long_pattern_signs, short_pattern_signs,
                              setup_events, recent_price_action
     HISTORICAL_EVIDENCE  -> identity, history, setup_events,
                              invalidation_evidence, memory
     RISK_EXECUTION       -> identity, price, levels, structure, regime,
                              policy_flags, memory, invalidation_evidence
     CHAIR                -> all slices above

   No dynamic SQL. No writes. Reads PROPOSAL_BOARD_DOSSIER_PACK_CACHE only.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.GET_PHASE4_DOSSIER_SLICE(
    RUN_ID      VARCHAR,
    DOSSIER_ID  NUMBER,
    ROLE_NAME   VARCHAR,
    SLICE_NAME  VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_slice'
AS
$$
import json

_ALLOWED_ROLES = {
    'MARKET_STRUCTURE',
    'LEVEL_PRICE_ACTION',
    'THESIS',
    'HISTORICAL_EVIDENCE',
    'RISK_EXECUTION',
    'CHAIR',
}

_ALLOWED_SLICES = {
    'identity',
    'price',
    'recent_bars',
    'candle_sequence',
    'recent_price_action',
    'levels',
    'structure',
    'regime',
    'long_pattern_signs',
    'short_pattern_signs',
    'setup_events',
    'invalidation_evidence',
    'history',
    'memory',
    'policy_flags',
}

_ROLE_SLICE_MAP = {
    'MARKET_STRUCTURE': {
        'identity', 'price', 'recent_bars', 'candle_sequence',
        'recent_price_action', 'structure', 'regime',
    },
    'LEVEL_PRICE_ACTION': {
        'identity', 'price', 'recent_bars', 'candle_sequence',
        'recent_price_action', 'levels',
    },
    'THESIS': {
        'identity', 'price', 'structure', 'regime', 'levels',
        'long_pattern_signs', 'short_pattern_signs',
        'setup_events', 'recent_price_action',
    },
    'HISTORICAL_EVIDENCE': {
        'identity', 'history', 'setup_events',
        'invalidation_evidence', 'memory',
    },
    'RISK_EXECUTION': {
        'identity', 'price', 'levels', 'structure', 'regime',
        'policy_flags', 'memory', 'invalidation_evidence',
    },
    'CHAIR': {
        'identity', 'price', 'recent_bars', 'candle_sequence',
        'recent_price_action', 'levels', 'structure', 'regime',
        'long_pattern_signs', 'short_pattern_signs',
        'setup_events', 'invalidation_evidence',
        'history', 'memory', 'policy_flags',
    },
}

# Map slice_name -> dossier payload key (most are identical names).
_SLICE_TO_PAYLOAD_KEY = {
    'identity': 'identity',
    'price': 'price',
    'recent_bars': 'recent_bars',
    'candle_sequence': 'candle_sequence',
    'recent_price_action': 'recent_price_action_summary',
    'levels': 'levels',
    'structure': 'structure',
    'regime': 'regime',
    'long_pattern_signs': 'long_pattern_signs',
    'short_pattern_signs': 'short_pattern_signs',
    'setup_events': 'setup_events_evidence_only',
    'invalidation_evidence': 'invalidation_evidence',
    'history': 'history',
    'memory': 'memory',
    'policy_flags': 'policy',
}


def get_slice(session, run_id: str, dossier_id: int, role_name: str, slice_name: str):
    role_upper = (role_name or '').strip().upper()
    slice_lower = (slice_name or '').strip().lower()
    rid = (run_id or '').strip()
    did = dossier_id

    if role_upper not in _ALLOWED_ROLES:
        return {'error': 'ROLE_NOT_ALLOWED', 'role': role_upper}
    if slice_lower not in _ALLOWED_SLICES:
        return {'error': 'SLICE_NOT_IN_CATALOG', 'slice': slice_lower}
    if slice_lower not in _ROLE_SLICE_MAP.get(role_upper, set()):
        return {'error': 'SLICE_NOT_ALLOWED_FOR_ROLE', 'role': role_upper, 'slice': slice_lower}
    if not rid:
        return {'error': 'RUN_ID_REQUIRED'}
    if did is None:
        return {'error': 'DOSSIER_ID_REQUIRED'}

    rows = session.sql(
        "SELECT PACK_JSON FROM MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE "
        "WHERE RUN_ID = ? AND DOSSIER_ID = ? AND EXPIRES_AT > CURRENT_TIMESTAMP()",
        params=[rid, int(did)],
    ).collect()

    if not rows:
        return {
            'error': 'PACK_NOT_FOUND_OR_EXPIRED',
            'run_id': rid,
            'dossier_id': int(did),
        }

    pack = rows[0]['PACK_JSON']
    if isinstance(pack, str):
        pack = json.loads(pack)

    payload_key = _SLICE_TO_PAYLOAD_KEY[slice_lower]
    slice_value = pack.get(payload_key) if isinstance(pack, dict) else None

    return {
        'run_id': rid,
        'dossier_id': int(did),
        'role': role_upper,
        'slice': slice_lower,
        'payload': slice_value,
    }
$$;

GRANT USAGE ON PROCEDURE MIP.APP.GET_PHASE4_DOSSIER_SLICE(VARCHAR, NUMBER, VARCHAR, VARCHAR) TO ROLE MIP_ADMIN_ROLE;

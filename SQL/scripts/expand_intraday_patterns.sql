-- ETF patterns (similar to STOCK but with slightly tighter thresholds for index products)
INSERT INTO MIP.APP.PATTERN_DEFINITION (PATTERN_ID, NAME, PATTERN_TYPE, ENABLED, IS_ACTIVE, PARAMS_JSON)
SELECT 302, 'ORB_ETF_15MIN', 'ORB', true, 'Y',
       parse_json('{"pattern_type":"ORB","market_type":"ETF","interval_minutes":15,"range_bars":2,"min_range_pct":0.002,"breakout_buffer_pct":0.001,"direction":"BOTH","session_start_hour_utc":14}');

INSERT INTO MIP.APP.PATTERN_DEFINITION (PATTERN_ID, NAME, PATTERN_TYPE, ENABLED, IS_ACTIVE, PARAMS_JSON)
SELECT 403, 'PULLBACK_ETF_15MIN', 'PULLBACK_CONTINUATION', true, 'Y',
       parse_json('{"pattern_type":"PULLBACK_CONTINUATION","market_type":"ETF","interval_minutes":15,"impulse_bars":3,"impulse_min_return":0.008,"consolidation_max_bars":3,"consolidation_max_range_pct":0.004,"breakout_buffer_pct":0.001}');

INSERT INTO MIP.APP.PATTERN_DEFINITION (PATTERN_ID, NAME, PATTERN_TYPE, ENABLED, IS_ACTIVE, PARAMS_JSON)
SELECT 405, 'MEANREV_ETF_15MIN', 'MEAN_REVERSION', true, 'Y',
       parse_json('{"pattern_type":"MEAN_REVERSION","market_type":"ETF","interval_minutes":15,"anchor_window":5,"min_bars_for_anchor":3,"deviation_threshold_pct":0.012,"direction":"BOTH"}');

-- FX patterns (tighter thresholds -- FX moves are smaller in percentage terms)
INSERT INTO MIP.APP.PATTERN_DEFINITION (PATTERN_ID, NAME, PATTERN_TYPE, ENABLED, IS_ACTIVE, PARAMS_JSON)
SELECT 303, 'ORB_FX_15MIN', 'ORB', true, 'Y',
       parse_json('{"pattern_type":"ORB","market_type":"FX","interval_minutes":15,"range_bars":2,"min_range_pct":0.001,"breakout_buffer_pct":0.0005,"direction":"BOTH","session_start_hour_utc":14}');

INSERT INTO MIP.APP.PATTERN_DEFINITION (PATTERN_ID, NAME, PATTERN_TYPE, ENABLED, IS_ACTIVE, PARAMS_JSON)
SELECT 404, 'PULLBACK_FX_15MIN', 'PULLBACK_CONTINUATION', true, 'Y',
       parse_json('{"pattern_type":"PULLBACK_CONTINUATION","market_type":"FX","interval_minutes":15,"impulse_bars":3,"impulse_min_return":0.003,"consolidation_max_bars":3,"consolidation_max_range_pct":0.002,"breakout_buffer_pct":0.0005}');

INSERT INTO MIP.APP.PATTERN_DEFINITION (PATTERN_ID, NAME, PATTERN_TYPE, ENABLED, IS_ACTIVE, PARAMS_JSON)
SELECT 406, 'MEANREV_FX_15MIN', 'MEAN_REVERSION', true, 'Y',
       parse_json('{"pattern_type":"MEAN_REVERSION","market_type":"FX","interval_minutes":15,"anchor_window":5,"min_bars_for_anchor":3,"deviation_threshold_pct":0.005,"direction":"BOTH"}');

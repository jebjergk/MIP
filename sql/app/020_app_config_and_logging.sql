CREATE TABLE IF NOT EXISTS MIP.APP.APP_CONFIG (
    CONFIG_KEY STRING COMMENT 'Unique identifier for the configuration value',
    CONFIG_VALUE STRING COMMENT 'Configuration value stored as text',
    DESCRIPTION STRING COMMENT 'Description of what the configuration controls',
    UPDATED_AT TIMESTAMP_NTZ COMMENT 'Timestamp of the most recent configuration update'
);

CREATE TABLE IF NOT EXISTS MIP.APP.PATTERN_DEFINITION (
    PATTERN_ID NUMBER AUTOINCREMENT COMMENT 'Unique identifier for a detection or trading pattern',
    NAME STRING COMMENT 'Human-readable name of the pattern',
    DESCRIPTION STRING COMMENT 'Detailed description of the pattern logic',
    ENABLED BOOLEAN COMMENT 'Flag indicating whether the pattern is active',
    CREATED_AT TIMESTAMP_NTZ COMMENT 'Timestamp when the pattern definition was created',
    CREATED_BY STRING COMMENT 'Identifier for the user or process that created the pattern'
);

CREATE TABLE IF NOT EXISTS MIP.APP.RECOMMENDATION_LOG (
    REC_ID NUMBER AUTOINCREMENT COMMENT 'Unique identifier for a generated recommendation',
    PATTERN_ID NUMBER COMMENT 'Reference to the pattern that produced the recommendation',
    PAIR STRING COMMENT 'FX pair (matching FX_TICKS.PAIR) evaluated for the recommendation',
    TS TIMESTAMP_NTZ COMMENT 'Timestamp representing when the recommendation was generated',
    SCORE NUMBER COMMENT 'Confidence or ranking score assigned to the recommendation',
    DETAILS VARIANT COMMENT 'Structured metadata describing how the recommendation was derived',
    CREATED_AT TIMESTAMP_NTZ COMMENT 'Timestamp when the recommendation was logged'
);

CREATE TABLE IF NOT EXISTS MIP.APP.OUTCOME_EVALUATION (
    OUTCOME_ID NUMBER AUTOINCREMENT COMMENT 'Unique identifier for the outcome evaluation entry',
    REC_ID NUMBER COMMENT 'Reference to the recommendation being evaluated',
    EVAL_TS TIMESTAMP_NTZ COMMENT 'Timestamp of when the outcome evaluation was performed',
    OUTCOME_SCORE NUMBER COMMENT 'Score or metric indicating recommendation performance',
    NOTES STRING COMMENT 'Additional notes or qualitative assessment of the outcome'
);

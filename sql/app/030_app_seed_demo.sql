-- Stored procedure to seed demo configuration and pattern data for the Market Intelligence Platform (MIP).
-- Run as a one-off initializer or as needed to refresh baseline demo settings without duplicating rows.
CREATE OR REPLACE PROCEDURE MIP.APP.SP_SEED_MIP_DEMO()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_upserted STRING;
BEGIN
    -- Ensure the default spread threshold configuration exists with the desired value/description.
    MERGE INTO MIP.APP.APP_CONFIG AS tgt
    USING (
        SELECT
            'DEFAULT_SPREAD_THRESHOLD' AS CONFIG_KEY,
            '0.0005' AS CONFIG_VALUE,
            'Default spread threshold for demo wide-spread signal.' AS DESCRIPTION,
            CURRENT_TIMESTAMP AS UPDATED_AT
    ) AS src
    ON tgt.CONFIG_KEY = src.CONFIG_KEY
    WHEN MATCHED THEN
        UPDATE SET
            CONFIG_VALUE = src.CONFIG_VALUE,
            DESCRIPTION = src.DESCRIPTION,
            UPDATED_AT = src.UPDATED_AT
    WHEN NOT MATCHED THEN
        INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
        VALUES (src.CONFIG_KEY, src.CONFIG_VALUE, src.DESCRIPTION, src.UPDATED_AT);

    -- Ensure the wide spread demo pattern definition exists exactly once.
    MERGE INTO MIP.APP.PATTERN_DEFINITION AS tgt
    USING (
        SELECT
            'WIDE_SPREAD_DEMO' AS NAME,
            'Flags FX ticks where spread exceeds a configured threshold.' AS DESCRIPTION,
            TRUE AS ENABLED,
            CURRENT_TIMESTAMP AS CREATED_AT,
            'SYSTEM' AS CREATED_BY
    ) AS src
    ON tgt.NAME = src.NAME
    WHEN MATCHED THEN
        UPDATE SET
            DESCRIPTION = src.DESCRIPTION,
            ENABLED = src.ENABLED
    WHEN NOT MATCHED THEN
        INSERT (NAME, DESCRIPTION, ENABLED, CREATED_AT, CREATED_BY)
        VALUES (src.NAME, src.DESCRIPTION, src.ENABLED, src.CREATED_AT, src.CREATED_BY);

    v_rows_upserted := 'Seed procedure completed successfully.';
    RETURN v_rows_upserted;
END;
$$;

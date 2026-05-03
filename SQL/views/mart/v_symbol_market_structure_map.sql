-- v_symbol_market_structure_map.sql
-- Phase 4 deterministic market structure evidence (wick pivots pivot_k=2, body-close BOS/CHOCH).
-- Grain: one row per (SYMBOL, MARKET_TYPE) present on daily MARKET_BARS anchor.
-- CHOCH v1: first body-close break vs dominant prior structure as-of latest bar (conservative).
--
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

CREATE OR REPLACE VIEW MIP.MART.V_SYMBOL_MARKET_STRUCTURE_MAP AS
WITH msm_anchor AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        MAX(TS) AS ANCHOR_TS
    FROM MIP.MART.MARKET_BARS
    WHERE INTERVAL_MINUTES = 1440
    GROUP BY SYMBOL, MARKET_TYPE
),
bars_raw AS (
    SELECT
        b.SYMBOL,
        b.MARKET_TYPE,
        b.TS,
        b.OPEN AS bar_open,
        b.HIGH AS bar_high,
        b.LOW AS bar_low,
        b.CLOSE AS bar_close,
        b.VOLUME AS bar_volume
    FROM MIP.MART.MARKET_BARS b
    INNER JOIN msm_anchor a
        ON a.SYMBOL = b.SYMBOL
       AND a.MARKET_TYPE = b.MARKET_TYPE
    WHERE b.INTERVAL_MINUTES = 1440
      AND b.TS >= DATEADD('day', -90, a.ANCHOR_TS)
),
bars AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        TS,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        bar_volume,
        GREATEST(bar_open, bar_close) AS body_high,
        LEAST(bar_open, bar_close) AS body_low,
        COUNT(*) OVER (PARTITION BY SYMBOL, MARKET_TYPE) AS bar_count_window
    FROM bars_raw
),
bars_l AS (
    SELECT
        b.*,
        LAG(bar_high, 1) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS h_m1,
        LAG(bar_high, 2) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS h_m2,
        LEAD(bar_high, 1) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS h_p1,
        LEAD(bar_high, 2) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS h_p2,
        LAG(bar_low, 1) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS l_m1,
        LAG(bar_low, 2) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS l_m2,
        LEAD(bar_low, 1) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS l_p1,
        LEAD(bar_low, 2) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY TS) AS l_p2
    FROM bars b
),
pivot_flags AS (
    SELECT
        l.*,
        IFF(
            h_m1 IS NOT NULL AND h_m2 IS NOT NULL AND h_p1 IS NOT NULL AND h_p2 IS NOT NULL
            AND bar_high > h_m1 AND bar_high > h_m2 AND bar_high > h_p1 AND bar_high > h_p2,
            TRUE,
            FALSE
        ) AS is_pivot_high,
        IFF(
            l_m1 IS NOT NULL AND l_m2 IS NOT NULL AND l_p1 IS NOT NULL AND l_p2 IS NOT NULL
            AND bar_low < l_m1 AND bar_low < l_m2 AND bar_low < l_p1 AND bar_low < l_p2,
            TRUE,
            FALSE
        ) AS is_pivot_low
    FROM bars_l l
),
pivot_high_rows AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        TS AS bar_ts,
        bar_high AS pivot_price,
        'HIGH'::VARCHAR AS pivot_kind,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        body_high,
        body_low
    FROM pivot_flags
    WHERE is_pivot_high
),
pivot_low_rows AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        TS AS bar_ts,
        bar_low AS pivot_price,
        'LOW'::VARCHAR AS pivot_kind,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        body_high,
        body_low
    FROM pivot_flags
    WHERE is_pivot_low
),
pivot_high_seq AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        bar_ts,
        pivot_price,
        pivot_kind,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        body_high,
        body_low,
        LAG(pivot_price) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY bar_ts) AS prev_high_price
    FROM pivot_high_rows
),
high_labeled AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        bar_ts,
        pivot_price,
        pivot_kind,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        body_high,
        body_low,
        IFF(prev_high_price IS NULL, 'SH',
            IFF(pivot_price > prev_high_price, 'HH',
                IFF(pivot_price < prev_high_price, 'LH', 'SH'))) AS swing_type
    FROM pivot_high_seq
),
pivot_low_seq AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        bar_ts,
        pivot_price,
        pivot_kind,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        body_high,
        body_low,
        LAG(pivot_price) OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY bar_ts) AS prev_low_price
    FROM pivot_low_rows
),
low_labeled AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        bar_ts,
        pivot_price,
        pivot_kind,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        body_high,
        body_low,
        IFF(prev_low_price IS NULL, 'SL',
            IFF(pivot_price > prev_low_price, 'HL',
                IFF(pivot_price < prev_low_price, 'LL', 'SL'))) AS swing_type
    FROM pivot_low_seq
),
all_swings AS (
    SELECT * FROM high_labeled
    UNION ALL
    SELECT * FROM low_labeled
),
all_swings_ord AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY SYMBOL, MARKET_TYPE
            ORDER BY bar_ts, IFF(pivot_kind = 'HIGH', 0, 1), pivot_price
        ) AS swing_ord,
        IFF(bar_high > bar_low,
            (bar_close - bar_low) / NULLIF(bar_high - bar_low, 0),
            NULL) AS close_pct_low_to_high
    FROM all_swings s
),
swing_counts AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        SUM(IFF(swing_type = 'HH', 1, 0)) AS cnt_hh,
        SUM(IFF(swing_type = 'HL', 1, 0)) AS cnt_hl,
        SUM(IFF(swing_type = 'LH', 1, 0)) AS cnt_lh,
        SUM(IFF(swing_type = 'LL', 1, 0)) AS cnt_ll,
        SUM(IFF(swing_type = 'SH', 1, 0)) AS cnt_sh,
        SUM(IFF(swing_type = 'SL', 1, 0)) AS cnt_sl,
        COUNT(*) AS pivot_total
    FROM all_swings_ord
    GROUP BY SYMBOL, MARKET_TYPE
),
recent_high_rows AS (
    SELECT *
    FROM all_swings_ord
    WHERE pivot_kind = 'HIGH'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY bar_ts DESC) <= 6
),
recent_high_mix2 AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        SUM(IFF(swing_type = 'HH', 1, 0)) AS r_hh,
        SUM(IFF(swing_type = 'LH', 1, 0)) AS r_lh
    FROM recent_high_rows
    GROUP BY SYMBOL, MARKET_TYPE
),
recent_low_rows AS (
    SELECT *
    FROM all_swings_ord
    WHERE pivot_kind = 'LOW'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY bar_ts DESC) <= 6
),
recent_low_mix2 AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        SUM(IFF(swing_type = 'HL', 1, 0)) AS r_hl,
        SUM(IFF(swing_type = 'LL', 1, 0)) AS r_ll
    FROM recent_low_rows
    GROUP BY SYMBOL, MARKET_TYPE
),
structure_kind AS (
    SELECT
        k.SYMBOL,
        k.MARKET_TYPE,
        IFF(
            COALESCE(h.r_hh, 0) >= COALESCE(h.r_lh, 0) + 1
            AND COALESCE(l.r_hl, 0) >= COALESCE(l.r_ll, 0) + 1,
            'UPTREND_HH_HL',
            IFF(
                COALESCE(h.r_lh, 0) >= COALESCE(h.r_hh, 0) + 1
                AND COALESCE(l.r_ll, 0) >= COALESCE(l.r_hl, 0) + 1,
                'DOWNTREND_LL_LH',
                IFF(
                    ABS(COALESCE(h.r_hh, 0) - COALESCE(h.r_lh, 0)) <= 1
                    AND ABS(COALESCE(l.r_hl, 0) - COALESCE(l.r_ll, 0)) <= 1,
                    'RANGE_STRUCTURE',
                    'MIXED_STRUCTURE'
                )
            )
        ) AS primary_structure
    FROM msm_anchor k
    LEFT JOIN recent_high_mix2 h
        ON h.SYMBOL = k.SYMBOL AND h.MARKET_TYPE = k.MARKET_TYPE
    LEFT JOIN recent_low_mix2 l
        ON l.SYMBOL = k.SYMBOL AND l.MARKET_TYPE = k.MARKET_TYPE
),
latest_bar_px AS (
    SELECT
        b.SYMBOL,
        b.MARKET_TYPE,
        b.TS AS last_ts,
        b.bar_open AS last_o,
        b.bar_high AS last_h,
        b.bar_low AS last_l,
        b.bar_close AS last_c,
        b.body_high AS last_bh,
        b.body_low AS last_bl,
        b.bar_count_window
    FROM bars b
    INNER JOIN msm_anchor a
        ON a.SYMBOL = b.SYMBOL
       AND a.MARKET_TYPE = b.MARKET_TYPE
       AND b.TS = a.ANCHOR_TS
),
prior_swing_high AS (
    SELECT
        ph.SYMBOL,
        ph.MARKET_TYPE,
        ph.pivot_price AS prior_sh_price,
        ph.bar_ts AS prior_sh_ts
    FROM high_labeled ph
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = ph.SYMBOL
       AND lb.MARKET_TYPE = ph.MARKET_TYPE
    WHERE ph.bar_ts < lb.last_ts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ph.SYMBOL, ph.MARKET_TYPE ORDER BY ph.bar_ts DESC) = 1
),
prior_swing_low AS (
    SELECT
        pl.SYMBOL,
        pl.MARKET_TYPE,
        pl.pivot_price AS prior_sl_price,
        pl.bar_ts AS prior_sl_ts
    FROM low_labeled pl
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = pl.SYMBOL
       AND lb.MARKET_TYPE = pl.MARKET_TYPE
    WHERE pl.bar_ts < lb.last_ts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pl.SYMBOL, pl.MARKET_TYPE ORDER BY pl.bar_ts DESC) = 1
),
last_hl_level AS (
    SELECT
        pl.SYMBOL,
        pl.MARKET_TYPE,
        pl.pivot_price AS last_hl_price,
        pl.bar_ts AS last_hl_ts
    FROM low_labeled pl
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = pl.SYMBOL
       AND lb.MARKET_TYPE = pl.MARKET_TYPE
    WHERE pl.bar_ts <= lb.last_ts
      AND pl.swing_type = 'HL'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pl.SYMBOL, pl.MARKET_TYPE ORDER BY pl.bar_ts DESC) = 1
),
last_lh_level AS (
    SELECT
        ph.SYMBOL,
        ph.MARKET_TYPE,
        ph.pivot_price AS last_lh_price,
        ph.bar_ts AS last_lh_ts
    FROM high_labeled ph
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = ph.SYMBOL
       AND lb.MARKET_TYPE = ph.MARKET_TYPE
    WHERE ph.bar_ts <= lb.last_ts
      AND ph.swing_type = 'LH'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ph.SYMBOL, ph.MARKET_TYPE ORDER BY ph.bar_ts DESC) = 1
),
range_bounds AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        MIN(bar_low) AS range_low,
        MAX(bar_high) AS range_high,
        COUNT(*) AS bars_in_win
    FROM bars_raw
    GROUP BY SYMBOL, MARKET_TYPE
),
bos_flags AS (
    SELECT
        lb.SYMBOL,
        lb.MARKET_TYPE,
        IFF(psh.prior_sh_price IS NOT NULL AND lb.last_c > psh.prior_sh_price, TRUE, FALSE) AS bullish_bos,
        IFF(psl.prior_sl_price IS NOT NULL AND lb.last_c < psl.prior_sl_price, TRUE, FALSE) AS bearish_bos
    FROM latest_bar_px lb
    LEFT JOIN prior_swing_high psh
        ON psh.SYMBOL = lb.SYMBOL AND psh.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN prior_swing_low psl
        ON psl.SYMBOL = lb.SYMBOL AND psl.MARKET_TYPE = lb.MARKET_TYPE
),
bos_detail AS (
    SELECT
        bf.SYMBOL,
        bf.MARKET_TYPE,
        bf.bullish_bos,
        bf.bearish_bos,
        IFF(bf.bullish_bos AND NOT bf.bearish_bos, 'UP',
            IFF(bf.bearish_bos AND NOT bf.bullish_bos, 'DOWN', 'NONE')) AS last_bos_direction,
        IFF(
            (bf.bullish_bos AND NOT bf.bearish_bos) OR (bf.bearish_bos AND NOT bf.bullish_bos),
            lb.last_ts::DATE,
            NULL
        ) AS last_bos_date,
        IFF(bf.bullish_bos AND NOT bf.bearish_bos, psh.prior_sh_price,
            IFF(bf.bearish_bos AND NOT bf.bullish_bos, psl.prior_sl_price, NULL)) AS last_bos_level,
        (bf.bullish_bos OR bf.bearish_bos) AS any_bos,
        (bf.bullish_bos OR bf.bearish_bos) AS body_close_confirmed_bos
    FROM bos_flags bf
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = bf.SYMBOL AND lb.MARKET_TYPE = bf.MARKET_TYPE
    LEFT JOIN prior_swing_high psh
        ON psh.SYMBOL = bf.SYMBOL AND psh.MARKET_TYPE = bf.MARKET_TYPE
    LEFT JOIN prior_swing_low psl
        ON psl.SYMBOL = bf.SYMBOL AND psl.MARKET_TYPE = bf.MARKET_TYPE
),
wick_probe AS (
    SELECT
        lb.SYMBOL,
        lb.MARKET_TYPE,
        IFF(
            psh.prior_sh_price IS NOT NULL
            AND lb.last_h > psh.prior_sh_price
            AND lb.last_c <= psh.prior_sh_price,
            TRUE,
            FALSE
        ) AS probe_above,
        IFF(
            psl.prior_sl_price IS NOT NULL
            AND lb.last_l < psl.prior_sl_price
            AND lb.last_c >= psl.prior_sl_price,
            TRUE,
            FALSE
        ) AS probe_below
    FROM latest_bar_px lb
    LEFT JOIN prior_swing_high psh
        ON psh.SYMBOL = lb.SYMBOL AND psh.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN prior_swing_low psl
        ON psl.SYMBOL = lb.SYMBOL AND psl.MARKET_TYPE = lb.MARKET_TYPE
),
wick_hl_probe AS (
    SELECT
        lb.SYMBOL,
        lb.MARKET_TYPE,
        IFF(
            hl.last_hl_price IS NOT NULL
            AND lb.last_l < hl.last_hl_price
            AND lb.last_c >= hl.last_hl_price,
            TRUE,
            FALSE
        ) AS wick_probe_below_hl
    FROM latest_bar_px lb
    LEFT JOIN last_hl_level hl
        ON hl.SYMBOL = lb.SYMBOL AND hl.MARKET_TYPE = lb.MARKET_TYPE
),
choch_flags AS (
    SELECT
        lb.SYMBOL,
        lb.MARKET_TYPE,
        sk.primary_structure,
        lb.last_c,
        hl.last_hl_price,
        lh.last_lh_price,
        IFF(
            sk.primary_structure = 'UPTREND_HH_HL'
            AND hl.last_hl_price IS NOT NULL
            AND lb.last_c < hl.last_hl_price,
            TRUE,
            FALSE
        ) AS choch_down_body,
        IFF(
            sk.primary_structure = 'DOWNTREND_LL_LH'
            AND lh.last_lh_price IS NOT NULL
            AND lb.last_c > lh.last_lh_price,
            TRUE,
            FALSE
        ) AS choch_up_body
    FROM latest_bar_px lb
    INNER JOIN structure_kind sk
        ON sk.SYMBOL = lb.SYMBOL AND sk.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN last_hl_level hl
        ON hl.SYMBOL = lb.SYMBOL AND hl.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN last_lh_level lh
        ON lh.SYMBOL = lb.SYMBOL AND lh.MARKET_TYPE = lb.MARKET_TYPE
),
choch_detail AS (
    SELECT
        cf.SYMBOL,
        cf.MARKET_TYPE,
        (cf.choch_down_body OR cf.choch_up_body) AS choch_detected,
        IFF(cf.choch_up_body AND NOT cf.choch_down_body, 'UP',
            IFF(cf.choch_down_body AND NOT cf.choch_up_body, 'DOWN', 'NONE')) AS choch_direction,
        IFF(
            (cf.choch_up_body AND NOT cf.choch_down_body) OR (cf.choch_down_body AND NOT cf.choch_up_body),
            lb.last_ts::DATE,
            NULL
        ) AS choch_date,
        IFF(cf.choch_up_body AND NOT cf.choch_down_body, cf.last_lh_price,
            IFF(cf.choch_down_body AND NOT cf.choch_up_body, cf.last_hl_price, NULL)) AS choch_level
    FROM choch_flags cf
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = cf.SYMBOL AND lb.MARKET_TYPE = cf.MARKET_TYPE
),
structure_health_calc AS (
    SELECT
        sk.SYMBOL,
        sk.MARKET_TYPE,
        sk.primary_structure,
        cf.choch_down_body,
        cf.choch_up_body,
        wp.probe_above,
        wp.probe_below,
        whp.wick_probe_below_hl,
        IFF(
            sk.primary_structure IN ('RANGE_STRUCTURE', 'MIXED_STRUCTURE'),
            'UNCONFIRMED',
            IFF(
                cf.choch_down_body OR cf.choch_up_body,
                'BROKEN',
                IFF(
                    wp.probe_above OR wp.probe_below OR whp.wick_probe_below_hl,
                    'DEGRADED_BUT_ALIVE',
                    'CONFIRMED'
                )
            )
        ) AS structure_health
    FROM structure_kind sk
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = sk.SYMBOL AND lb.MARKET_TYPE = sk.MARKET_TYPE
    INNER JOIN choch_flags cf
        ON cf.SYMBOL = sk.SYMBOL AND cf.MARKET_TYPE = sk.MARKET_TYPE
    LEFT JOIN wick_probe wp
        ON wp.SYMBOL = sk.SYMBOL AND wp.MARKET_TYPE = sk.MARKET_TYPE
    LEFT JOIN wick_hl_probe whp
        ON whp.SYMBOL = sk.SYMBOL AND whp.MARKET_TYPE = sk.MARKET_TYPE
),
structure_posture_hint_calc AS (
    SELECT
        sh.SYMBOL,
        sh.MARKET_TYPE,
        IFF(
            sh.structure_health = 'BROKEN',
            'NOT_ACTIONABLE',
            IFF(
                sh.structure_health IN ('UNCONFIRMED', 'DEGRADED_BUT_ALIVE')
                    OR sh.primary_structure IN ('RANGE_STRUCTURE', 'MIXED_STRUCTURE'),
                'MONITOR',
                IFF(sh.structure_health = 'CONFIRMED', 'ACTIONABLE_PROPOSAL', 'UNCLASSIFIED')
            )
        ) AS structure_posture_hint
    FROM structure_health_calc sh
),
impulse_corr AS (
    SELECT
        lb.SYMBOL,
        lb.MARKET_TYPE,
        sk.primary_structure,
        bd.bullish_bos,
        bd.bearish_bos,
        psh.prior_sh_price,
        psl.prior_sl_price,
        hl.last_hl_price,
        IFF(
            sk.primary_structure = 'UPTREND_HH_HL',
            'UP',
            IFF(sk.primary_structure = 'DOWNTREND_LL_LH', 'DOWN', 'NONE')
        ) AS last_impulse_direction,
        IFF(
            sk.primary_structure = 'UPTREND_HH_HL'
            AND psh.prior_sh_price IS NOT NULL,
            lb.last_c < psh.prior_sh_price AND NOT bd.bullish_bos,
            IFF(
                sk.primary_structure = 'DOWNTREND_LL_LH'
                AND psl.prior_sl_price IS NOT NULL,
                lb.last_c > psl.prior_sl_price AND NOT bd.bearish_bos,
                FALSE
            )
        ) AS correction_active,
        IFF(
            sk.primary_structure = 'UPTREND_HH_HL'
            AND psh.prior_sh_price IS NOT NULL
            AND lb.last_c < psh.prior_sh_price,
            ROUND(
                100.0 * (psh.prior_sh_price - lb.last_c) / NULLIF(psh.prior_sh_price, 0),
                4
            ),
            IFF(
                sk.primary_structure = 'DOWNTREND_LL_LH'
                AND psl.prior_sl_price IS NOT NULL
                AND lb.last_c > psl.prior_sl_price,
                ROUND(
                    100.0 * (lb.last_c - psl.prior_sl_price) / NULLIF(psl.prior_sl_price, 0),
                    4
                ),
                0.0
            )
        ) AS correction_depth_pct,
        IFF(
            sk.primary_structure = 'UPTREND_HH_HL',
            hl.last_hl_price IS NOT NULL AND lb.last_c >= hl.last_hl_price,
            IFF(
                sk.primary_structure = 'DOWNTREND_LL_LH',
                lh.last_lh_price IS NOT NULL AND lb.last_c <= lh.last_lh_price,
                TRUE
            )
        ) AS correction_holds_structure,
        hl.last_hl_ts AS impulse_low_ref_ts,
        lh.last_lh_ts AS impulse_high_ref_ts
    FROM latest_bar_px lb
    INNER JOIN structure_kind sk
        ON sk.SYMBOL = lb.SYMBOL AND sk.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN bos_detail bd
        ON bd.SYMBOL = lb.SYMBOL AND bd.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN prior_swing_high psh
        ON psh.SYMBOL = lb.SYMBOL AND psh.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN prior_swing_low psl
        ON psl.SYMBOL = lb.SYMBOL AND psl.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN last_hl_level hl
        ON hl.SYMBOL = lb.SYMBOL AND hl.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN last_lh_level lh
        ON lh.SYMBOL = lb.SYMBOL AND lh.MARKET_TYPE = lb.MARKET_TYPE
),
consolidation_calc AS (
    SELECT
        sk.SYMBOL,
        sk.MARKET_TYPE,
        rb.range_low,
        rb.range_high,
        rb.bars_in_win,
        IFF(sk.primary_structure = 'RANGE_STRUCTURE', TRUE, FALSE) AS consolidation_active,
        IFF(sk.primary_structure = 'RANGE_STRUCTURE', rb.bars_in_win, 0) AS bars_in_range
    FROM structure_kind sk
    INNER JOIN range_bounds rb
        ON rb.SYMBOL = sk.SYMBOL AND rb.MARKET_TYPE = sk.MARKET_TYPE
),
phase_calc AS (
    SELECT
        sk.SYMBOL,
        sk.MARKET_TYPE,
        IFF(sk.primary_structure = 'RANGE_STRUCTURE', 'CONSOLIDATION',
            IFF(cd.choch_detected, 'REJECTION',
                IFF(ic.correction_active AND ic.last_impulse_direction = 'UP', 'CORRECTION_DOWN',
                    IFF(ic.correction_active AND ic.last_impulse_direction = 'DOWN', 'CORRECTION_UP',
                        IFF(ic.last_impulse_direction = 'UP', 'IMPULSE_UP',
                            IFF(ic.last_impulse_direction = 'DOWN', 'IMPULSE_DOWN', 'CONSOLIDATION')))))) AS current_phase
    FROM structure_kind sk
    LEFT JOIN choch_detail cd
        ON cd.SYMBOL = sk.SYMBOL AND cd.MARKET_TYPE = sk.MARKET_TYPE
    LEFT JOIN impulse_corr ic
        ON ic.SYMBOL = sk.SYMBOL AND ic.MARKET_TYPE = sk.MARKET_TYPE
),
latest_event AS (
    SELECT
        lb.SYMBOL,
        lb.MARKET_TYPE,
        bd.bullish_bos,
        bd.bearish_bos,
        cd.choch_detected,
        cd.choch_direction,
        wp.probe_above,
        wp.probe_below,
        sk.primary_structure,
        rb.range_high,
        rb.range_low,
        lb.last_h,
        lb.last_l,
        lb.last_c,
        IFF(
            bd.bullish_bos AND NOT bd.bearish_bos,
            'BODY_CLOSE_BOS_UP',
            IFF(
                bd.bearish_bos AND NOT bd.bullish_bos,
                'BODY_CLOSE_BOS_DOWN',
                IFF(
                    cd.choch_direction = 'UP',
                    'CHOCH_UP',
                    IFF(
                        cd.choch_direction = 'DOWN',
                        'CHOCH_DOWN',
                        IFF(
                            wp.probe_above,
                            'WICK_PROBE_ABOVE_STRUCTURE',
                            IFF(
                                wp.probe_below OR whp.wick_probe_below_hl,
                                'WICK_PROBE_BELOW_STRUCTURE',
                                IFF(
                                    sk.primary_structure = 'RANGE_STRUCTURE'
                                    AND lb.last_h > rb.range_high
                                    AND lb.last_c < rb.range_high,
                                    'FAILED_BREAKOUT',
                                    IFF(
                                        sk.primary_structure = 'RANGE_STRUCTURE'
                                        AND lb.last_l < rb.range_low
                                        AND lb.last_c > rb.range_low,
                                        'FAILED_BREAKDOWN',
                                        IFF(
                                            sk.primary_structure = 'RANGE_STRUCTURE',
                                            'RANGE_CONTINUATION',
                                            'STRUCTURE_HOLD'
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ) AS latest_structure_event
    FROM latest_bar_px lb
    LEFT JOIN bos_detail bd
        ON bd.SYMBOL = lb.SYMBOL AND bd.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN choch_detail cd
        ON cd.SYMBOL = lb.SYMBOL AND cd.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN wick_probe wp
        ON wp.SYMBOL = lb.SYMBOL AND wp.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN wick_hl_probe whp
        ON whp.SYMBOL = lb.SYMBOL AND whp.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN structure_kind sk
        ON sk.SYMBOL = lb.SYMBOL AND sk.MARKET_TYPE = lb.MARKET_TYPE
    LEFT JOIN range_bounds rb
        ON rb.SYMBOL = lb.SYMBOL AND rb.MARKET_TYPE = lb.MARKET_TYPE
),
swings_tail_agg AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'date', bar_ts::DATE,
                'price', pivot_price,
                'swing_type', swing_type,
                'body_confirmed',
                    IFF(pivot_kind = 'HIGH',
                        close_pct_low_to_high >= 0.5,
                        close_pct_low_to_high <= 0.5),
                'wick_probe',
                    IFF(pivot_kind = 'HIGH',
                        bar_high > body_high
                        AND (bar_high - body_high) > IFF(bar_high > bar_low, (bar_high - bar_low) * 0.2, 0),
                        bar_low < body_low
                        AND (body_low - bar_low) > IFF(bar_high > bar_low, (bar_high - bar_low) * 0.2, 0))
            )
        ) WITHIN GROUP (ORDER BY bar_ts ASC) AS major_swings_tail12
    FROM (
        SELECT *
        FROM all_swings_ord
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL, MARKET_TYPE ORDER BY bar_ts DESC) <= 12
    ) t
    GROUP BY SYMBOL, MARKET_TYPE
),
swings_full_audit AS (
    SELECT
        SYMBOL,
        MARKET_TYPE,
        ARRAY_AGG(
            OBJECT_CONSTRUCT_KEEP_NULL(
                'date', bar_ts::DATE,
                'price', pivot_price,
                'swing_type', swing_type,
                'body_confirmed',
                    IFF(pivot_kind = 'HIGH',
                        close_pct_low_to_high >= 0.5,
                        close_pct_low_to_high <= 0.5),
                'wick_probe',
                    IFF(pivot_kind = 'HIGH',
                        bar_high > body_high
                        AND (bar_high - body_high) > IFF(bar_high > bar_low, (bar_high - bar_low) * 0.2, 0),
                        bar_low < body_low
                        AND (body_low - bar_low) > IFF(bar_high > bar_low, (bar_high - bar_low) * 0.2, 0))
            )
        ) WITHIN GROUP (ORDER BY bar_ts ASC) AS major_swings_full
    FROM all_swings_ord
    GROUP BY SYMBOL, MARKET_TYPE
),
base_row AS (
    SELECT
        a.SYMBOL,
        a.MARKET_TYPE,
        a.ANCHOR_TS,
        lb.bar_count_window,
        sk.primary_structure,
        sh.structure_health,
        sp.structure_posture_hint,
        pc.current_phase,
        le.latest_structure_event,
        bd.bullish_bos,
        bd.bearish_bos,
        bd.last_bos_direction,
        bd.last_bos_date,
        bd.last_bos_level,
        bd.body_close_confirmed_bos,
        cd.choch_detected,
        cd.choch_direction,
        cd.choch_date,
        cd.choch_level,
        ic.last_impulse_direction,
        ic.impulse_low_ref_ts::DATE AS last_impulse_low_ref_date,
        ic.impulse_high_ref_ts::DATE AS last_impulse_high_ref_date,
        ic.correction_active,
        ic.correction_depth_pct,
        ic.correction_holds_structure,
        cc.consolidation_active,
        cc.range_low AS consol_range_low,
        cc.range_high AS consol_range_high,
        cc.bars_in_range,
        sc.cnt_hh,
        sc.cnt_hl,
        sc.cnt_lh,
        sc.cnt_ll,
        sc.cnt_sh,
        sc.cnt_sl,
        sc.pivot_total,
        sta.major_swings_tail12,
        sfa.major_swings_full,
        br.range_low AS window_range_low,
        br.range_high AS window_range_high
    FROM msm_anchor a
    INNER JOIN latest_bar_px lb
        ON lb.SYMBOL = a.SYMBOL AND lb.MARKET_TYPE = a.MARKET_TYPE
    INNER JOIN structure_kind sk
        ON sk.SYMBOL = a.SYMBOL AND sk.MARKET_TYPE = a.MARKET_TYPE
    INNER JOIN structure_health_calc sh
        ON sh.SYMBOL = a.SYMBOL AND sh.MARKET_TYPE = a.MARKET_TYPE
    INNER JOIN structure_posture_hint_calc sp
        ON sp.SYMBOL = a.SYMBOL AND sp.MARKET_TYPE = a.MARKET_TYPE
    INNER JOIN phase_calc pc
        ON pc.SYMBOL = a.SYMBOL AND pc.MARKET_TYPE = a.MARKET_TYPE
    INNER JOIN latest_event le
        ON le.SYMBOL = a.SYMBOL AND le.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN bos_detail bd
        ON bd.SYMBOL = a.SYMBOL AND bd.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN choch_detail cd
        ON cd.SYMBOL = a.SYMBOL AND cd.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN impulse_corr ic
        ON ic.SYMBOL = a.SYMBOL AND ic.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN consolidation_calc cc
        ON cc.SYMBOL = a.SYMBOL AND cc.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN swing_counts sc
        ON sc.SYMBOL = a.SYMBOL AND sc.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN swings_tail_agg sta
        ON sta.SYMBOL = a.SYMBOL AND sta.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN swings_full_audit sfa
        ON sfa.SYMBOL = a.SYMBOL AND sfa.MARKET_TYPE = a.MARKET_TYPE
    LEFT JOIN range_bounds br
        ON br.SYMBOL = a.SYMBOL AND br.MARKET_TYPE = a.MARKET_TYPE
)
SELECT
    SYMBOL,
    MARKET_TYPE,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'lookback_days', 90,
        'pivot_k', 2,
        'anchor_date', ANCHOR_TS::DATE,
        'start_date',
            DATEADD('day', -90, ANCHOR_TS::DATE),
        'bar_count', bar_count_window,
        'primary_structure', primary_structure,
        'structure_health', structure_health,
        'structure_posture_hint', structure_posture_hint,
        'current_phase', current_phase,
        'major_swings', COALESCE(major_swings_tail12, ARRAY_CONSTRUCT()),
        'swing_summary_counts',
            OBJECT_CONSTRUCT_KEEP_NULL(
                'HH', COALESCE(cnt_hh, 0),
                'HL', COALESCE(cnt_hl, 0),
                'LH', COALESCE(cnt_lh, 0),
                'LL', COALESCE(cnt_ll, 0),
                'SH', COALESCE(cnt_sh, 0),
                'SL', COALESCE(cnt_sl, 0),
                'pivot_total', COALESCE(pivot_total, 0)
            ),
        'latest_structure_event', latest_structure_event,
        'bos',
            OBJECT_CONSTRUCT_KEEP_NULL(
                'bullish_bos', COALESCE(bullish_bos, FALSE),
                'bearish_bos', COALESCE(bearish_bos, FALSE),
                'last_bos_direction', COALESCE(last_bos_direction, 'NONE'),
                'last_bos_date', last_bos_date,
                'last_bos_level', last_bos_level,
                'body_close_confirmed', COALESCE(body_close_confirmed_bos, FALSE)
            ),
        'choch',
            OBJECT_CONSTRUCT_KEEP_NULL(
                'detected', COALESCE(choch_detected, FALSE),
                'direction', COALESCE(choch_direction, 'NONE'),
                'date', choch_date,
                'level', choch_level
            ),
        'impulse_correction',
            OBJECT_CONSTRUCT_KEEP_NULL(
                'last_impulse_direction', COALESCE(last_impulse_direction, 'NONE'),
                'last_impulse_start',
                    IFF(
                        last_impulse_direction = 'UP',
                        last_impulse_low_ref_date,
                        last_impulse_high_ref_date
                    ),
                'last_impulse_end', ANCHOR_TS::DATE,
                'correction_active', COALESCE(correction_active, FALSE),
                'correction_depth_pct', COALESCE(correction_depth_pct, 0.0),
                'correction_holds_structure', COALESCE(correction_holds_structure, TRUE)
            ),
        'consolidation',
            OBJECT_CONSTRUCT_KEEP_NULL(
                'active', COALESCE(consolidation_active, FALSE),
                'range_low', IFF(consolidation_active, consol_range_low, NULL),
                'range_high', IFF(consolidation_active, consol_range_high, NULL),
                'bars_in_range', COALESCE(bars_in_range, 0)
            ),
        'body_vs_wick_rule',
            OBJECT_CONSTRUCT_KEEP_NULL(
                'body_close_required_for_structure_break', TRUE,
                'wick_breaks_structure', FALSE
            ),
        'interpretability',
            OBJECT_CONSTRUCT_KEEP_NULL(
                'major_swings_full', COALESCE(major_swings_full, ARRAY_CONSTRUCT()),
                'window_range_low', window_range_low,
                'window_range_high', window_range_high,
                'notes',
                    ARRAY_CONSTRUCT(
                        'Swing pivots use wick highs/lows with pivot_k=2.',
                        'BOS/CHOCH require daily close beyond referenced swing level.',
                        'structure_posture_hint is evidence-only (not operational_state).'
                    )
            )
    ) AS MARKET_STRUCTURE_MAP
FROM base_row;

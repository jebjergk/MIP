-- 188_sp_agent_propose_trades.sql
-- Purpose: Deterministic agent proposal generator with symbol-level trust gating.
--
-- IMPORTANT: Candidate selection starts from latest-day trusted candidates, then enforces
-- symbol-level trust (from V_TRAINING_DIGEST_SNAPSHOT_SYMBOL). No RUN_ID filter is applied since
-- the views are already date-scoped.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_PROPOSE_TRADES(
    P_PORTFOLIO_ID number,
    P_RUN_ID string,   -- pipeline run id for deterministic tie-back to recommendations
    P_PARENT_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_portfolio_profile_id number;
    v_max_positions number;
    v_max_position_pct float;
    v_candidate_count number := 0;
    v_open_positions number := 0;
    v_remaining_capacity number := 0;
    v_entries_blocked boolean := false;
    v_stop_reason string;
    v_allowed_actions string;
    v_inserted_count number := 0;
    v_selected_count number := 0;
    -- Proposal size should be profile-driven, not hardcoded globally.
    -- We set target_weight from MAX_POSITION_PCT so each portfolio strategy
    -- controls proposal sizing.
    v_target_weight float := 0.05;
    v_run_id_string string := :P_RUN_ID;
    v_current_bar_index number := 0;
    v_max_new_stock number := 0;
    v_max_new_fx number := 0;
    v_available_stock number := 0;
    v_available_fx number := 0;
    v_skipped_held_count number := 0;
    v_selected_stock number := 0;
    v_selected_fx number := 0;
    v_selected_etf number := 0;
    v_candidate_count_raw number := 0;
    v_candidate_count_trusted number := 0;
    v_trusted_rejected_count number := 0;
    -- Diagnostic variables for enhanced logging
    v_latest_bar_ts timestamp_ntz;
    v_latest_daily_bar_ts timestamp_ntz;
    v_latest_rec_ts timestamp_ntz;
    v_trusted_pattern_count number := 0;
    v_no_candidates_reason string := null;
    v_daily_stale_block_enabled boolean := true;
    v_daily_bar_max_age_hours number := 30;
    v_daily_bar_age_hours number := null;
    v_daily_data_stale boolean := false;
begin
    select
        p.PROFILE_ID,
        prof.MAX_POSITIONS,
        prof.MAX_POSITION_PCT
      into v_portfolio_profile_id,
           v_max_positions,
           v_max_position_pct
      from MIP.APP.PORTFOLIO p
      left join MIP.APP.PORTFOLIO_PROFILE prof
        on prof.PROFILE_ID = p.PROFILE_ID
     where p.PORTFOLIO_ID = :P_PORTFOLIO_ID;

    if (v_portfolio_profile_id is null) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Portfolio not found',
            'portfolio_id', :P_PORTFOLIO_ID
        );
    end if;

    v_max_positions := coalesce(v_max_positions, 5);
    v_max_position_pct := coalesce(v_max_position_pct, 0.05);
    v_target_weight := v_max_position_pct;

    if (v_max_positions <= 0) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Invalid max positions configuration',
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions
        );
    end if;

    select coalesce(
               max(bar_index),
               0
           )
      into :v_current_bar_index
      from (
        select
            BAR_INDEX
        from MIP.MART.V_BAR_INDEX
        qualify row_number() over (
            partition by TS
            order by BAR_INDEX
        ) = 1
        order by TS desc
        limit 1
    );

    select count(*)
      into :v_open_positions
      from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
     where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
       and p.CURRENT_BAR_INDEX = :v_current_bar_index;

    v_remaining_capacity := greatest(:v_max_positions - :v_open_positions, 0);

    select
        coalesce(max(ENTRIES_BLOCKED), false),
        max(STOP_REASON),
        max(ALLOWED_ACTIONS)
      into :v_entries_blocked,
           :v_stop_reason,
           :v_allowed_actions
      from MIP.MART.V_PORTFOLIO_RISK_STATE
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    -- COOLDOWN enforcement: block entries if still within cooldown window.
    -- COOLDOWN_UNTIL_TS is set by SP_CHECK_CRYSTALLIZE after profit target hit.
    if (not v_entries_blocked) then
        let v_cooldown_until timestamp_ntz := null;
        begin
            select COOLDOWN_UNTIL_TS into :v_cooldown_until
              from MIP.APP.PORTFOLIO
             where PORTFOLIO_ID = :P_PORTFOLIO_ID;
        exception when other then v_cooldown_until := null;
        end;
        if (v_cooldown_until is not null and current_timestamp() < v_cooldown_until) then
            v_entries_blocked := true;
            v_stop_reason := 'COOLDOWN';
            v_allowed_actions := 'ALLOW_EXITS_ONLY';
        end if;
    end if;

    -- Get diagnostic timestamps for logging
    v_latest_bar_ts := (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440);
    v_latest_daily_bar_ts := :v_latest_bar_ts;
    v_latest_rec_ts := (select max(TS) from MIP.APP.RECOMMENDATION_LOG where INTERVAL_MINUTES = 1440);
    v_trusted_pattern_count := (select count(*) from MIP.MART.V_TRUSTED_PATTERN_HORIZONS);
    begin
        select
            coalesce(max(iff(CONFIG_KEY = 'DAILY_BAR_STALE_BLOCK_ENABLED', try_to_boolean(CONFIG_VALUE), null)), true),
            coalesce(max(iff(CONFIG_KEY = 'DAILY_BAR_MAX_AGE_HOURS', try_to_number(CONFIG_VALUE), null)), 30)
          into :v_daily_stale_block_enabled, :v_daily_bar_max_age_hours
          from MIP.APP.APP_CONFIG
         where CONFIG_KEY in ('DAILY_BAR_STALE_BLOCK_ENABLED', 'DAILY_BAR_MAX_AGE_HOURS');
    exception when other then
        v_daily_stale_block_enabled := true;
        v_daily_bar_max_age_hours := 30;
    end;
    if (:v_latest_daily_bar_ts is not null) then
        v_daily_bar_age_hours := datediff('hour', :v_latest_daily_bar_ts, current_timestamp());
    end if;
    v_daily_data_stale := (
        :v_latest_daily_bar_ts is null
        or :v_daily_bar_age_hours is null
        or :v_daily_bar_age_hours > :v_daily_bar_max_age_hours
    );
    if (
        not :v_entries_blocked
        and :v_daily_stale_block_enabled
        and :v_daily_data_stale
    ) then
        v_entries_blocked := true;
        v_stop_reason := 'STALE_DAILY_BARS';
        v_allowed_actions := 'ALLOW_EXITS_ONLY';
    end if;

    if (v_entries_blocked) then
        -- Count raw signals at latest TS (no RUN_ID filter - views are already date-scoped)
        select count(*)
          into :v_candidate_count_raw
          from MIP.MART.V_SIGNALS_LATEST_TS;

        -- Count symbol-trusted signals (aligns with training page trust label).
        select count(*)
          into :v_candidate_count_trusted
          from (
              select distinct s.RECOMMENDATION_ID
              from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
              left join MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL snap
                on snap.SYMBOL = s.SYMBOL
               and snap.MARKET_TYPE = s.MARKET_TYPE
               and snap.PATTERN_ID = s.PATTERN_ID
              where upper(coalesce(snap.SNAPSHOT_JSON:trust:trust_label::string, 'UNTRUSTED')) = 'TRUSTED'
          );

        v_candidate_count := :v_candidate_count_trusted;
        v_trusted_rejected_count := greatest(:v_candidate_count_raw - :v_candidate_count_trusted, 0);
        v_selected_count := least(:v_candidate_count, :v_remaining_capacity);

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            PARENT_RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS
        )
        select
            current_timestamp(),
            :v_run_id_string,
            :P_PARENT_RUN_ID,
            'AGENT',
            'SP_AGENT_PROPOSE_TRADES',
            'SKIP_ENTRIES_BLOCKED',
            0,
            object_construct(
                'entries_blocked', :v_entries_blocked,
                'stop_reason', :v_stop_reason,
                'allowed_actions', :v_allowed_actions,
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'candidate_count', :v_candidate_count,
                'proposed_count', :v_selected_count,
                'candidate_count_raw', :v_candidate_count_raw,
                'candidate_count_trusted', :v_candidate_count_trusted,
                'trusted_rejected_count', :v_trusted_rejected_count,
                'diagnostics', object_construct(
                    'latest_bar_ts', :v_latest_bar_ts,
                    'latest_daily_bar_ts', :v_latest_daily_bar_ts,
                    'daily_bar_age_hours', :v_daily_bar_age_hours,
                    'daily_bar_max_age_hours', :v_daily_bar_max_age_hours,
                    'daily_stale_block_enabled', :v_daily_stale_block_enabled,
                    'daily_data_stale', :v_daily_data_stale,
                    'latest_rec_ts', :v_latest_rec_ts,
                    'trusted_pattern_count', :v_trusted_pattern_count,
                    'rec_ts_matches_bar_ts', :v_latest_rec_ts = :v_latest_bar_ts
                )
            );

        return object_construct(
            'status', 'SKIP_ENTRIES_BLOCKED',
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'entries_blocked', :v_entries_blocked,
            'stop_reason', :v_stop_reason,
            'allowed_actions', :v_allowed_actions,
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'proposal_candidates', :v_candidate_count,
            'proposal_selected', :v_selected_count,
            'proposal_inserted', 0,
            'target_weight', :v_target_weight
        );
    end if;

    -- Count raw signals at latest TS (no RUN_ID filter - views are already date-scoped)
    select count(*)
      into :v_candidate_count_raw
      from MIP.MART.V_SIGNALS_LATEST_TS;

    -- Count symbol-trusted signals (aligns with training page trust label).
    select count(*)
      into :v_candidate_count_trusted
      from (
          select distinct s.RECOMMENDATION_ID
          from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
          left join MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL snap
            on snap.SYMBOL = s.SYMBOL
           and snap.MARKET_TYPE = s.MARKET_TYPE
           and snap.PATTERN_ID = s.PATTERN_ID
          where upper(coalesce(snap.SNAPSHOT_JSON:trust:trust_label::string, 'UNTRUSTED')) = 'TRUSTED'
      );

    v_candidate_count := :v_candidate_count_trusted;
    v_trusted_rejected_count := greatest(:v_candidate_count_raw - :v_candidate_count_trusted, 0);
    v_selected_count := least(:v_candidate_count, :v_remaining_capacity);

    -- Determine reason if no candidates
    if (v_candidate_count = 0) then
        if (v_candidate_count_raw = 0) then
            if (v_latest_rec_ts is null) then
                v_no_candidates_reason := 'NO_RECOMMENDATIONS_IN_LOG';
            elseif (v_latest_rec_ts != v_latest_bar_ts) then
                v_no_candidates_reason := 'REC_TS_STALE';
            else
                v_no_candidates_reason := 'NO_SIGNALS_AT_LATEST_TS';
            end if;
        elseif (v_trusted_pattern_count = 0) then
            v_no_candidates_reason := 'NO_TRUSTED_PATTERNS';
        else
            v_no_candidates_reason := 'SIGNALS_NOT_SYMBOL_TRUSTED';
        end if;
    elseif (v_remaining_capacity = 0) then
        v_no_candidates_reason := 'MAX_POSITIONS_REACHED';
    end if;

    if (v_candidate_count = 0 or v_remaining_capacity = 0) then
        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            PARENT_RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS
        )
        select
            current_timestamp(),
            :v_run_id_string,
            :P_PARENT_RUN_ID,
            'AGENT',
            'SP_AGENT_PROPOSE_TRADES',
            'INFO',
            0,
            object_construct(
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'candidate_count', :v_candidate_count,
                'proposed_count', :v_selected_count,
                'candidate_count_raw', :v_candidate_count_raw,
                'candidate_count_trusted', :v_candidate_count_trusted,
                'trusted_rejected_count', :v_trusted_rejected_count,
                'no_candidates_reason', :v_no_candidates_reason,
                'diagnostics', object_construct(
                    'latest_bar_ts', :v_latest_bar_ts,
                    'latest_rec_ts', :v_latest_rec_ts,
                    'trusted_pattern_count', :v_trusted_pattern_count,
                    'rec_ts_matches_bar_ts', :v_latest_rec_ts = :v_latest_bar_ts
                )
            );

        return object_construct(
            'status', iff(v_candidate_count = 0, 'NO_ELIGIBLE_SIGNALS', 'NO_CAPACITY'),
            'run_id', :P_RUN_ID,
            'portfolio_id', :P_PORTFOLIO_ID,
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'proposal_candidates', :v_candidate_count,
            'proposal_selected', :v_selected_count,
            'proposal_inserted', 0,
            'target_weight', :v_target_weight,
            'no_candidates_reason', :v_no_candidates_reason,
            'diagnostics', object_construct(
                'latest_bar_ts', :v_latest_bar_ts,
                'latest_rec_ts', :v_latest_rec_ts,
                'trusted_pattern_count', :v_trusted_pattern_count,
                'rec_ts_matches_bar_ts', :v_latest_rec_ts = :v_latest_bar_ts
            )
        );
    end if;

    v_max_new_stock := ceil(:v_remaining_capacity * 0.6);
    v_max_new_fx := :v_remaining_capacity - :v_max_new_stock;

    -- Keep quota defaults to avoid additional optimizer-sensitive scans.
    v_available_stock := 1;
    v_available_fx := 1;

    -- Defensive fallback: skipped-held diagnostic is non-critical and can be
    -- left as zero to avoid optimizer/internal errors in this branch.
    v_skipped_held_count := 0;

    merge into MIP.AGENT_OUT.ORDER_PROPOSALS as target
    using (
        with held_symbols as (
            select distinct
                p.SYMBOL
            from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
            where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
              and p.CURRENT_BAR_INDEX = :v_current_bar_index
        ),
        news_cfg as (
            select
                coalesce(max(iff(CONFIG_KEY = 'NEWS_ENABLED', lower(CONFIG_VALUE), null)), 'false') as NEWS_ENABLED,
                coalesce(max(iff(CONFIG_KEY = 'NEWS_DISPLAY_ONLY', lower(CONFIG_VALUE), null)), 'true') as NEWS_DISPLAY_ONLY,
                coalesce(max(iff(CONFIG_KEY = 'NEWS_INFLUENCE_ENABLED', lower(CONFIG_VALUE), null)), 'false') as NEWS_INFLUENCE_ENABLED,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_DECAY_TAU_HOURS', CONFIG_VALUE, null))), 24) as NEWS_DECAY_TAU_HOURS,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_PRESSURE_HOT', CONFIG_VALUE, null))), 0.12) as NEWS_PRESSURE_HOT,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_UNCERTAINTY_HIGH', CONFIG_VALUE, null))), 0.08) as NEWS_UNCERTAINTY_HIGH,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_EVENT_RISK_HIGH', CONFIG_VALUE, null))), 0.10) as NEWS_EVENT_RISK_HIGH,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_SCORE_MAX_ABS', CONFIG_VALUE, null))), 0.20) as NEWS_SCORE_MAX_ABS,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_HOT_PRESSURE', CONFIG_VALUE, null))), 2.5) as NEWS_HOT_PRESSURE,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_CONFLICT_HIGH', CONFIG_VALUE, null))), 0.60) as NEWS_CONFLICT_HIGH,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_SCORE_CAP', CONFIG_VALUE, null))), 0.20) as NEWS_SCORE_CAP,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_SIZE_MULT_CAUTION', CONFIG_VALUE, null))), 0.75) as NEWS_SIZE_MULT_CAUTION,
                coalesce(max(iff(CONFIG_KEY = 'NEWS_BLOCK_ON_CONFLICT', lower(CONFIG_VALUE), null)), 'true') as NEWS_BLOCK_ON_CONFLICT,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_STALENESS_THRESHOLD_MINUTES', CONFIG_VALUE, null))), 180) as NEWS_STALE_MINUTES,
                coalesce(max(iff(CONFIG_KEY = 'SYMBOL_LOCAL_GATE_ENABLED', lower(CONFIG_VALUE), null)), 'true') as SYMBOL_LOCAL_GATE_ENABLED,
                coalesce(max(iff(CONFIG_KEY = 'SYMBOL_LOCAL_GATE_ENFORCE', lower(CONFIG_VALUE), null)), 'false') as SYMBOL_LOCAL_GATE_ENFORCE,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'SYMBOL_LOCAL_MIN_RECENT_HIT_RATE', CONFIG_VALUE, null))), 0.50) as SYMBOL_LOCAL_MIN_RECENT_HIT_RATE,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'SYMBOL_LOCAL_MIN_RECENT_AVG_RETURN', CONFIG_VALUE, null))), 0.00) as SYMBOL_LOCAL_MIN_RECENT_AVG_RETURN,
                coalesce(max(try_to_number(iff(CONFIG_KEY = 'SYMBOL_LOCAL_MIN_RECS', CONFIG_VALUE, null))), 8) as SYMBOL_LOCAL_MIN_RECS
            from MIP.APP.APP_CONFIG
        ),
        news_candidates as (
            select
                s.RECOMMENDATION_ID,
                n.NEWS_COUNT,
                n.NEWS_CONTEXT_BADGE,
                n.NOVELTY_SCORE,
                n.BURST_SCORE,
                n.UNCERTAINTY_FLAG,
                n.TOP_HEADLINES,
                n.LAST_NEWS_PUBLISHED_AT,
                n.LAST_INGESTED_AT,
                n.SNAPSHOT_TS,
                row_number() over (
                    partition by s.RECOMMENDATION_ID
                    order by n.SNAPSHOT_TS desc, n.CREATED_AT desc
                ) as RN
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
            left join MIP.NEWS.NEWS_INFO_STATE_DAILY n
              on n.SYMBOL = s.SYMBOL
             and n.MARKET_TYPE = s.MARKET_TYPE
             and n.SNAPSHOT_TS <= s.SIGNAL_TS
        ),
        news_latest as (
            select
                RECOMMENDATION_ID,
                NEWS_COUNT,
                NEWS_CONTEXT_BADGE,
                NOVELTY_SCORE,
                BURST_SCORE,
                UNCERTAINTY_FLAG,
                TOP_HEADLINES,
                LAST_NEWS_PUBLISHED_AT,
                LAST_INGESTED_AT,
                SNAPSHOT_TS
            from news_candidates
            where RN = 1
        ),
        news_feature_candidates as (
            select
                s.RECOMMENDATION_ID,
                f.EVENT_COUNT,
                f.NEWS_PRESSURE,
                f.NEWS_SENTIMENT,
                f.UNCERTAINTY_SCORE,
                f.EVENT_RISK_SCORE,
                f.MACRO_HEAT,
                f.TOP_EVENTS,
                f.NEWS_SNAPSHOT_AGE_MINUTES,
                f.NEWS_IS_STALE,
                f.SNAPSHOT_TS,
                row_number() over (
                    partition by s.RECOMMENDATION_ID
                    order by f.AS_OF_TS desc, f.SNAPSHOT_TS desc
                ) as RN
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
            left join MIP.MART.V_NEWS_FEATURES_BY_TS f
              on f.SYMBOL = s.SYMBOL
             and f.MARKET_TYPE = s.MARKET_TYPE
             and f.AS_OF_TS <= s.SIGNAL_TS
        ),
        news_feature_latest as (
            select
                RECOMMENDATION_ID,
                EVENT_COUNT,
                NEWS_PRESSURE,
                NEWS_SENTIMENT,
                UNCERTAINTY_SCORE,
                EVENT_RISK_SCORE,
                MACRO_HEAT,
                TOP_EVENTS,
                NEWS_SNAPSHOT_AGE_MINUTES as FEATURE_SNAPSHOT_AGE_MINUTES,
                NEWS_IS_STALE as FEATURE_IS_STALE,
                SNAPSHOT_TS as FEATURE_SNAPSHOT_TS
            from news_feature_candidates
            where RN = 1
        ),
        news_agg_candidates as (
            select
                s.RECOMMENDATION_ID,
                a.INFO_PRESSURE,
                a.NOVELTY,
                a.CONFLICT,
                a.BADGE,
                a.TOP_CLUSTERS,
                a.LAST_PUBLISHED_AT,
                a.LAST_INGESTED_AT,
                a.AS_OF_TS_BUCKET,
                row_number() over (
                    partition by s.RECOMMENDATION_ID
                    order by a.AS_OF_TS_BUCKET desc
                ) as RN
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
            left join MIP.NEWS.NEWS_AGGREGATED_EVENTS a
              on a.SYMBOL = s.SYMBOL
             and a.MARKET_TYPE = s.MARKET_TYPE
             and a.AS_OF_TS_BUCKET <= s.SIGNAL_TS
        ),
        news_agg_latest as (
            select
                RECOMMENDATION_ID,
                INFO_PRESSURE,
                NOVELTY,
                CONFLICT,
                BADGE,
                TOP_CLUSTERS,
                LAST_PUBLISHED_AT,
                LAST_INGESTED_AT,
                AS_OF_TS_BUCKET
            from news_agg_candidates
            where RN = 1
        ),
        eligible_candidates as (
            select
                s.*,
                cfg.NEWS_ENABLED,
                cfg.NEWS_DISPLAY_ONLY,
                cfg.NEWS_INFLUENCE_ENABLED,
                cfg.NEWS_DECAY_TAU_HOURS,
                cfg.NEWS_PRESSURE_HOT,
                cfg.NEWS_UNCERTAINTY_HIGH,
                cfg.NEWS_EVENT_RISK_HIGH,
                cfg.NEWS_SCORE_MAX_ABS,
                cfg.NEWS_HOT_PRESSURE,
                cfg.NEWS_CONFLICT_HIGH,
                cfg.NEWS_SCORE_CAP,
                cfg.NEWS_SIZE_MULT_CAUTION,
                cfg.NEWS_BLOCK_ON_CONFLICT,
                cfg.NEWS_STALE_MINUTES,
                nl.NEWS_COUNT as NEWS_COUNT,
                nl.NEWS_CONTEXT_BADGE as NEWS_CONTEXT_BADGE,
                nl.NOVELTY_SCORE as NEWS_NOVELTY_SCORE,
                nl.BURST_SCORE as NEWS_BURST_SCORE,
                nl.UNCERTAINTY_FLAG as NEWS_UNCERTAINTY_FLAG,
                nl.TOP_HEADLINES as NEWS_TOP_HEADLINES,
                nl.LAST_NEWS_PUBLISHED_AT as NEWS_LAST_PUBLISHED_AT,
                nl.LAST_INGESTED_AT as NEWS_LAST_INGESTED_AT,
                nl.SNAPSHOT_TS as NEWS_SNAPSHOT_TS,
                nfl.EVENT_COUNT as NEWS_FEATURE_EVENT_COUNT,
                nfl.NEWS_PRESSURE as NEWS_FEATURE_PRESSURE,
                nfl.NEWS_SENTIMENT as NEWS_FEATURE_SENTIMENT,
                nfl.UNCERTAINTY_SCORE as NEWS_FEATURE_UNCERTAINTY_SCORE,
                nfl.EVENT_RISK_SCORE as NEWS_FEATURE_EVENT_RISK_SCORE,
                nfl.MACRO_HEAT as NEWS_FEATURE_MACRO_HEAT,
                nfl.TOP_EVENTS as NEWS_FEATURE_TOP_EVENTS,
                nfl.FEATURE_SNAPSHOT_TS as NEWS_FEATURE_SNAPSHOT_TS,
                na.INFO_PRESSURE as NEWS_AGG_INFO_PRESSURE,
                na.NOVELTY as NEWS_AGG_NOVELTY,
                na.CONFLICT as NEWS_AGG_CONFLICT,
                na.BADGE as NEWS_AGG_BADGE,
                na.TOP_CLUSTERS as NEWS_AGG_TOP_CLUSTERS,
                na.AS_OF_TS_BUCKET as NEWS_AGG_BUCKET_TS,
                cfg.SYMBOL_LOCAL_GATE_ENABLED,
                cfg.SYMBOL_LOCAL_GATE_ENFORCE,
                cfg.SYMBOL_LOCAL_MIN_RECENT_HIT_RATE,
                cfg.SYMBOL_LOCAL_MIN_RECENT_AVG_RETURN,
                cfg.SYMBOL_LOCAL_MIN_RECS,
                iff(
                    coalesce(nfl.FEATURE_SNAPSHOT_TS, nl.SNAPSHOT_TS) is null,
                    null,
                    datediff('minute', coalesce(nfl.FEATURE_SNAPSHOT_TS, nl.SNAPSHOT_TS), s.SIGNAL_TS)
                ) as NEWS_SNAPSHOT_AGE_MINUTES,
                iff(
                    coalesce(nfl.FEATURE_IS_STALE, iff(coalesce(nfl.FEATURE_SNAPSHOT_TS, nl.SNAPSHOT_TS) is null, null, datediff('minute', coalesce(nfl.FEATURE_SNAPSHOT_TS, nl.SNAPSHOT_TS), s.SIGNAL_TS) > cfg.NEWS_STALE_MINUTES)) is null,
                    null,
                    coalesce(nfl.FEATURE_IS_STALE, datediff('minute', coalesce(nfl.FEATURE_SNAPSHOT_TS, nl.SNAPSHOT_TS), s.SIGNAL_TS) > cfg.NEWS_STALE_MINUTES)
                ) as NEWS_IS_STALE,
                case
                    when s.MARKET_TYPE = 'FX' then 'FX'
                    else 'STOCK'
                end as MARKET_TYPE_GROUP
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
            -- No RUN_ID filter - view is already date-scoped to latest TS
            cross join news_cfg cfg
            left join news_latest nl
              on nl.RECOMMENDATION_ID = s.RECOMMENDATION_ID
            left join news_feature_latest nfl
              on nfl.RECOMMENDATION_ID = s.RECOMMENDATION_ID
            left join news_agg_latest na
              on na.RECOMMENDATION_ID = s.RECOMMENDATION_ID
        ),
        symbol_local_health as (
            select
                e.*,
                snap.SNAPSHOT_JSON:trust:trust_label::string as SYMBOL_LOCAL_TRUST_LABEL,
                coalesce(
                    try_to_double(snap.SNAPSHOT_JSON:trust:reason:recent_hit_rate::string),
                    try_to_double(snap.SNAPSHOT_JSON:evidence:hit_rate::string)
                ) as SYMBOL_LOCAL_RECENT_HIT_RATE,
                coalesce(
                    try_to_double(snap.SNAPSHOT_JSON:trust:reason:recent_avg_return::string),
                    try_to_double(snap.SNAPSHOT_JSON:evidence:avg_return::string)
                ) as SYMBOL_LOCAL_RECENT_AVG_RETURN,
                coalesce(
                    try_to_number(snap.SNAPSHOT_JSON:evidence:recs_total::string),
                    0
                ) as SYMBOL_LOCAL_RECS_TOTAL,
                coalesce(
                    snap.SNAPSHOT_JSON:threshold_gaps:hit_rate_met::boolean,
                    false
                ) as SYMBOL_LOCAL_HIT_RATE_MET,
                coalesce(
                    snap.SNAPSHOT_JSON:threshold_gaps:avg_return_met::boolean,
                    false
                ) as SYMBOL_LOCAL_AVG_RETURN_MET,
                coalesce(
                    snap.SNAPSHOT_JSON:threshold_gaps:signals_met::boolean,
                    false
                ) as SYMBOL_LOCAL_SIGNALS_MET
            from eligible_candidates e
            left join MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL snap
              on snap.SYMBOL = e.SYMBOL
             and snap.MARKET_TYPE = e.MARKET_TYPE
             and snap.PATTERN_ID = e.PATTERN_ID
        ),
        symbol_local_gated as (
            select
                d.*
            from symbol_local_health d
            where upper(coalesce(d.SYMBOL_LOCAL_TRUST_LABEL, 'UNTRUSTED')) = 'TRUSTED'
        ),
        deduped_candidates as (
            select
                d.*
            from symbol_local_gated d
            qualify row_number() over (
                partition by d.SYMBOL
                order by d.SCORE desc, d.RECOMMENDATION_ID
            ) = 1
        ),
        enriched_news as (
            select
                d.*,
                iff(
                    d.SYMBOL_LOCAL_GATE_ENABLED = 'true'
                    and (
                        coalesce(d.SYMBOL_LOCAL_RECENT_HIT_RATE, -1) < d.SYMBOL_LOCAL_MIN_RECENT_HIT_RATE
                        or coalesce(d.SYMBOL_LOCAL_RECENT_AVG_RETURN, -999) < d.SYMBOL_LOCAL_MIN_RECENT_AVG_RETURN
                        or coalesce(d.SYMBOL_LOCAL_RECS_TOTAL, 0) < d.SYMBOL_LOCAL_MIN_RECS
                    ),
                    true,
                    false
                ) as SYMBOL_LOCAL_GATE_WOULD_BLOCK,
                iff(
                    d.NEWS_SNAPSHOT_AGE_MINUTES is null,
                    0.0,
                    exp(
                        -greatest(d.NEWS_SNAPSHOT_AGE_MINUTES, 0) / 60.0
                        / nullif(d.NEWS_DECAY_TAU_HOURS, 0)
                    )
                ) as NEWS_RECENCY_WEIGHT,
                case upper(coalesce(d.NEWS_CONTEXT_BADGE, ''))
                    when 'HOT' then 1.0
                    when 'WARM' then 0.5
                    when 'COLD' then -0.25
                    else 0.0
                end as LEGACY_NEWS_PRESSURE_SCORE,
                coalesce(
                    iff(
                        coalesce(d.NEWS_AGG_INFO_PRESSURE, 0) > 0 and abs(coalesce(d.NEWS_HOT_PRESSURE, 0)) > 0,
                        least(d.NEWS_AGG_INFO_PRESSURE / nullif(abs(d.NEWS_HOT_PRESSURE), 0), 2.0),
                        null
                    ),
                    d.NEWS_FEATURE_SENTIMENT,
                    case upper(coalesce(d.NEWS_CONTEXT_BADGE, ''))
                        when 'HOT' then 1.0
                        when 'WARM' then 0.5
                        when 'COLD' then -0.25
                        else 0.0
                    end
                ) as NEWS_PRESSURE_SCORE,
                coalesce(
                    d.NEWS_FEATURE_UNCERTAINTY_SCORE,
                    iff(coalesce(d.NEWS_UNCERTAINTY_FLAG, false), 0.7, 0.0)
                ) as NEWS_UNCERTAINTY_PROXY,
                least(
                    greatest(
                        greatest(
                            coalesce(d.NEWS_FEATURE_EVENT_RISK_SCORE, d.NEWS_BURST_SCORE, 0.0),
                            coalesce(d.NEWS_AGG_CONFLICT, 0.0),
                            coalesce(d.NEWS_FEATURE_UNCERTAINTY_SCORE, iff(coalesce(d.NEWS_UNCERTAINTY_FLAG, false), 0.7, 0.0)),
                            iff(coalesce(d.NEWS_IS_STALE, false), 1.0, 0.0),
                            coalesce(d.NEWS_FEATURE_MACRO_HEAT, 0.0)
                        ),
                        0.0
                    ),
                    1.0
                ) as NEWS_EVENT_RISK_PROXY
            from deduped_candidates d
        ),
        scored_candidates as (
            select
                e.*,
                array_construct_compact(
                    iff(upper(coalesce(e.NEWS_CONTEXT_BADGE, '')) = 'HOT', 'HOT_CONTEXT', null),
                    iff(upper(coalesce(e.NEWS_CONTEXT_BADGE, '')) = 'WARM', 'WARM_CONTEXT', null),
                    iff(coalesce(e.NEWS_UNCERTAINTY_PROXY, 0) >= 0.60, 'UNCERTAINTY_HIGH', null),
                    iff(coalesce(e.NEWS_IS_STALE, false), 'SNAPSHOT_STALE', null),
                    iff(e.NEWS_EVENT_RISK_PROXY >= 0.90, 'EVENT_RISK_HIGH', null),
                    iff(coalesce(e.NEWS_FEATURE_MACRO_HEAT, 0) >= 0.60, 'MACRO_HEAT_HIGH', null),
                    iff(coalesce(e.NEWS_AGG_CONFLICT, 0) >= coalesce(e.NEWS_CONFLICT_HIGH, 0.60), 'CONFLICT_HIGH', null)
                ) as NEWS_REASONS,
                least(
                    greatest(
                        iff(
                            e.NEWS_ENABLED = 'true',
                            e.NEWS_RECENCY_WEIGHT
                            * (
                                e.NEWS_PRESSURE_SCORE * abs(e.NEWS_PRESSURE_HOT)
                                - (coalesce(e.NEWS_UNCERTAINTY_PROXY, 0.0) * abs(e.NEWS_UNCERTAINTY_HIGH))
                                - (e.NEWS_EVENT_RISK_PROXY * abs(e.NEWS_EVENT_RISK_HIGH))
                            ),
                            0.0
                        ),
                        -abs(coalesce(e.NEWS_SCORE_CAP, e.NEWS_SCORE_MAX_ABS))
                    ),
                    abs(coalesce(e.NEWS_SCORE_CAP, e.NEWS_SCORE_MAX_ABS))
                ) as NEWS_SCORE_ADJ_SHADOW,
                -- News is diagnostic context at proposal time. Proposal ranking
                -- and blocking are trust-gate driven; committee handles news.
                0.0 as NEWS_SCORE_ADJ_APPLIED
            from enriched_news e
        ),
        prioritized as (
            select
                s.*,
                iff(h.SYMBOL is null, 0, 1) as HELD_PRIORITY,
                s.SCORE as FINAL_SCORE,
                false as NEWS_BLOCK_NEW_ENTRY
            from scored_candidates s
            left join held_symbols h
              on h.SYMBOL = s.SYMBOL
        ),
        ranked as (
            select
                p.*,
                row_number() over (
                    order by
                        p.HELD_PRIORITY asc,
                        p.FINAL_SCORE desc,
                        p.SCORE desc,
                        p.RECOMMENDATION_ID
                ) as OVERALL_RANK,
                row_number() over (
                    partition by p.MARKET_TYPE_GROUP
                    order by
                        p.HELD_PRIORITY asc,
                        p.FINAL_SCORE desc,
                        p.SCORE desc,
                        p.RECOMMENDATION_ID
                ) as TYPE_RANK
            from prioritized p
        ),
        stock_pass as (
            select
                r.*
            from ranked r
            where r.MARKET_TYPE_GROUP = 'STOCK'
              and r.TYPE_RANK <= :v_max_new_stock
        ),
        fx_pass as (
            select
                r.*
            from ranked r
            where r.MARKET_TYPE_GROUP = 'FX'
              and r.TYPE_RANK <= :v_max_new_fx
        ),
        quota_selected as (
            select * from stock_pass
            union all
            select * from fx_pass
        ),
        quota_limited as (
            select
                q.*,
                row_number() over (
                    order by q.OVERALL_RANK
                ) as QUOTA_ORDER
            from quota_selected q
        ),
        primary_selected as (
            select
                q.*
            from quota_limited q
            where q.QUOTA_ORDER <= :v_remaining_capacity
        ),
        remaining_slots as (
            select greatest(
                :v_remaining_capacity - (select count(*) from primary_selected),
                0
            ) as SLOTS
        ),
        backfill_candidates as (
            select
                r.*
            from ranked r
            left join primary_selected p
              on p.RECOMMENDATION_ID = r.RECOMMENDATION_ID
            where p.RECOMMENDATION_ID is null
        ),
        backfill_ranked as (
            select
                b.*,
                row_number() over (
                    order by b.OVERALL_RANK
                ) as BACKFILL_RANK
            from backfill_candidates b
        ),
        backfill_selected as (
            select
                b.*
            from backfill_ranked b
            cross join remaining_slots rs
            where b.BACKFILL_RANK <= rs.SLOTS
        ),
        final_selected as (
            select * from primary_selected
            union all
            select * from backfill_selected
        ),
        final_ranked as (
            select
                f.*,
                row_number() over (
                    order by f.OVERALL_RANK
                ) as SELECTION_RANK
            from final_selected f
        )
        select
            :P_RUN_ID as RUN_ID_VARCHAR,
            :P_PORTFOLIO_ID as PORTFOLIO_ID,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.INTERVAL_MINUTES,
            'BUY' as SIDE,
            greatest(
                0.01,
                least(
                    :v_max_position_pct,
                    :v_target_weight
                )
            ) as TARGET_WEIGHT,
            s.RECOMMENDATION_ID,
            s.SIGNAL_TS,
            s.PATTERN_ID as SIGNAL_PATTERN_ID,
            s.INTERVAL_MINUTES as SIGNAL_INTERVAL_MINUTES,
            s.RUN_ID as SIGNAL_RUN_ID,
            s.DETAILS as SIGNAL_SNAPSHOT,
            object_construct(
                'recommendation_id', s.RECOMMENDATION_ID,
                'pattern_id', s.PATTERN_ID,
                'ts', s.SIGNAL_TS,
                'score', s.SCORE,
                'interval_minutes', s.INTERVAL_MINUTES,
                'run_id', s.RUN_ID,
                'trust_label', coalesce(s.SYMBOL_LOCAL_TRUST_LABEL, 'UNKNOWN'),
                'recommended_action', 'ENABLE',
                'training_version', s.TRAINING_VERSION,
                'pattern_target', s.PATTERN_TARGET,
                'symbol_multiplier', s.SYMBOL_MULTIPLIER,
                'effective_target', s.EFFECTIVE_TARGET,
                'target_source', s.TARGET_SOURCE,
                'held_priority', s.HELD_PRIORITY,
                'market_type_group', s.MARKET_TYPE_GROUP,
                'trust_reason', 'SYMBOL_LOCAL_TRUSTED',
                'base_score', s.SCORE,
                'final_score', s.FINAL_SCORE,
                'news_enabled', iff(s.NEWS_ENABLED = 'true', true, false),
                'news_display_only', iff(s.NEWS_DISPLAY_ONLY = 'true', true, false),
                'news_influence_enabled', iff(s.NEWS_INFLUENCE_ENABLED = 'true', true, false),
                'symbol_local_gate_enabled', iff(s.SYMBOL_LOCAL_GATE_ENABLED = 'true', true, false),
                'symbol_local_gate_enforce', iff(s.SYMBOL_LOCAL_GATE_ENFORCE = 'true', true, false),
                'symbol_local_gate_would_block', s.SYMBOL_LOCAL_GATE_WOULD_BLOCK,
                'symbol_local_trust_label', s.SYMBOL_LOCAL_TRUST_LABEL,
                'symbol_local_recent_hit_rate', s.SYMBOL_LOCAL_RECENT_HIT_RATE,
                'symbol_local_recent_avg_return', s.SYMBOL_LOCAL_RECENT_AVG_RETURN,
                'symbol_local_recs_total', s.SYMBOL_LOCAL_RECS_TOTAL,
                'symbol_local_min_recent_hit_rate', s.SYMBOL_LOCAL_MIN_RECENT_HIT_RATE,
                'symbol_local_min_recent_avg_return', s.SYMBOL_LOCAL_MIN_RECENT_AVG_RETURN,
                'symbol_local_min_recs', s.SYMBOL_LOCAL_MIN_RECS,
                'symbol_local_hit_rate_met', s.SYMBOL_LOCAL_HIT_RATE_MET,
                'symbol_local_avg_return_met', s.SYMBOL_LOCAL_AVG_RETURN_MET,
                'symbol_local_signals_met', s.SYMBOL_LOCAL_SIGNALS_MET,
                'news_decay_tau_hours', s.NEWS_DECAY_TAU_HOURS,
                'news_pressure_hot', s.NEWS_PRESSURE_HOT,
                'news_uncertainty_high', s.NEWS_UNCERTAINTY_HIGH,
                'news_event_risk_high', s.NEWS_EVENT_RISK_HIGH,
                'news_score_max_abs', s.NEWS_SCORE_MAX_ABS,
                'news_hot_pressure', s.NEWS_HOT_PRESSURE,
                'news_conflict_high', s.NEWS_CONFLICT_HIGH,
                'news_score_cap', s.NEWS_SCORE_CAP,
                'news_size_mult_caution', s.NEWS_SIZE_MULT_CAUTION,
                'news_block_on_conflict', iff(s.NEWS_BLOCK_ON_CONFLICT = 'true', true, false),
                'news_score_adj_shadow', s.NEWS_SCORE_ADJ_SHADOW,
                'news_score_adj_applied', s.NEWS_SCORE_ADJ_APPLIED,
                'news_score_adj', s.NEWS_SCORE_ADJ_APPLIED,
                'news_influence_applied', iff(
                    s.NEWS_ENABLED = 'true'
                    and s.NEWS_INFLUENCE_ENABLED = 'true'
                    and s.NEWS_DISPLAY_ONLY <> 'true',
                    true,
                    false
                ),
                'news_event_risk_proxy', s.NEWS_EVENT_RISK_PROXY,
                'news_uncertainty_proxy', s.NEWS_UNCERTAINTY_PROXY,
                'news_agg', iff(
                    s.NEWS_AGG_BUCKET_TS is null,
                    null,
                    object_construct(
                        'bucket_ts', s.NEWS_AGG_BUCKET_TS,
                        'badge', s.NEWS_AGG_BADGE,
                        'info_pressure', s.NEWS_AGG_INFO_PRESSURE,
                        'novelty', s.NEWS_AGG_NOVELTY,
                        'conflict', s.NEWS_AGG_CONFLICT,
                        'top_clusters', s.NEWS_AGG_TOP_CLUSTERS,
                        'last_published_at', s.NEWS_LAST_PUBLISHED_AT,
                        'last_ingested_at', s.NEWS_LAST_INGESTED_AT
                    )
                ),
                'news_block_new_entry', s.NEWS_BLOCK_NEW_ENTRY,
                'news_reasons', s.NEWS_REASONS,
                'news_staleness_threshold_minutes', s.NEWS_STALE_MINUTES,
                'news_snapshot_age_minutes', s.NEWS_SNAPSHOT_AGE_MINUTES,
                'news_is_stale', s.NEWS_IS_STALE,
                'news_features', object_construct(
                    'event_count', s.NEWS_FEATURE_EVENT_COUNT,
                    'news_pressure', s.NEWS_FEATURE_PRESSURE,
                    'news_sentiment', s.NEWS_FEATURE_SENTIMENT,
                    'uncertainty_score', s.NEWS_FEATURE_UNCERTAINTY_SCORE,
                    'event_risk_score', s.NEWS_FEATURE_EVENT_RISK_SCORE,
                    'macro_heat', s.NEWS_FEATURE_MACRO_HEAT,
                    'top_events', s.NEWS_FEATURE_TOP_EVENTS,
                    'snapshot_ts', s.NEWS_FEATURE_SNAPSHOT_TS
                ),
                'news_context', iff(
                    s.NEWS_ENABLED = 'true' and s.NEWS_SNAPSHOT_TS is not null,
                    object_construct(
                        'news_count', s.NEWS_COUNT,
                        'news_context_badge', s.NEWS_CONTEXT_BADGE,
                        'novelty_score', s.NEWS_NOVELTY_SCORE,
                        'burst_score', s.NEWS_BURST_SCORE,
                        'uncertainty_flag', s.NEWS_UNCERTAINTY_FLAG,
                        'top_headlines', s.NEWS_TOP_HEADLINES,
                        'last_news_published_at', s.NEWS_LAST_PUBLISHED_AT,
                        'last_ingested_at', s.NEWS_LAST_INGESTED_AT,
                        'snapshot_ts', s.NEWS_SNAPSHOT_TS
                    ),
                    null
                )
            ) as SOURCE_SIGNALS,
            object_construct(
                'strategy', 'diversified_capacity_aware_top_n',
                'max_positions', :v_max_positions,
                'open_positions', :v_open_positions,
                'remaining_capacity', :v_remaining_capacity,
                'max_position_pct', :v_max_position_pct,
                'market_type_quota', object_construct(
                    'STOCK', :v_max_new_stock,
                    'FX', :v_max_new_fx
                ),
                'selection_rank', s.SELECTION_RANK,
                'base_score', s.SCORE,
                'final_score', s.FINAL_SCORE,
                'news_score_adj_shadow', s.NEWS_SCORE_ADJ_SHADOW,
                'news_score_adj_applied', s.NEWS_SCORE_ADJ_APPLIED,
                'news_score_adj', s.NEWS_SCORE_ADJ_APPLIED,
                'symbol_local_gate_would_block', s.SYMBOL_LOCAL_GATE_WOULD_BLOCK,
                'symbol_local_recent_hit_rate', s.SYMBOL_LOCAL_RECENT_HIT_RATE,
                'symbol_local_recent_avg_return', s.SYMBOL_LOCAL_RECENT_AVG_RETURN,
                'symbol_local_recs_total', s.SYMBOL_LOCAL_RECS_TOTAL,
                'news_agg_badge', s.NEWS_AGG_BADGE,
                'news_agg_info_pressure', s.NEWS_AGG_INFO_PRESSURE,
                'news_agg_conflict', s.NEWS_AGG_CONFLICT,
                'news_influence_applied', iff(
                    s.NEWS_ENABLED = 'true'
                    and s.NEWS_INFLUENCE_ENABLED = 'true'
                    and s.NEWS_DISPLAY_ONLY <> 'true',
                    true,
                    false
                ),
                'news_block_new_entry', s.NEWS_BLOCK_NEW_ENTRY,
                'news_reasons', s.NEWS_REASONS,
                'news_snapshot_age_minutes', s.NEWS_SNAPSHOT_AGE_MINUTES,
                'news_is_stale', s.NEWS_IS_STALE
            ) as RATIONALE
        from final_ranked s
        where s.SELECTION_RANK <= :v_remaining_capacity
    ) as source
    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
   and target.RECOMMENDATION_ID = source.RECOMMENDATION_ID
    when matched and target.STATUS = 'PROPOSED' then update set
        target.RUN_ID_VARCHAR = source.RUN_ID_VARCHAR,
        target.RATIONALE = source.RATIONALE,
        target.SOURCE_SIGNALS = source.SOURCE_SIGNALS
    when not matched then
        insert (
            RUN_ID_VARCHAR,
            PORTFOLIO_ID,
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            SIDE,
            TARGET_WEIGHT,
            RECOMMENDATION_ID,
            SIGNAL_TS,
            SIGNAL_PATTERN_ID,
            SIGNAL_INTERVAL_MINUTES,
            SIGNAL_RUN_ID,
            SIGNAL_SNAPSHOT,
            SOURCE_SIGNALS,
            RATIONALE,
            STATUS
        )
        values (
            source.RUN_ID_VARCHAR,
            source.PORTFOLIO_ID,
            source.SYMBOL,
            source.MARKET_TYPE,
            source.INTERVAL_MINUTES,
            source.SIDE,
            source.TARGET_WEIGHT,
            source.RECOMMENDATION_ID,
            source.SIGNAL_TS,
            source.SIGNAL_PATTERN_ID,
            source.SIGNAL_INTERVAL_MINUTES,
            source.SIGNAL_RUN_ID,
            source.SIGNAL_SNAPSHOT,
            source.SOURCE_SIGNALS,
            source.RATIONALE,
            'PROPOSED'
        );

    -- Count proposals inserted for this run (SQLROWCOUNT not reliable after MERGE)
    select
        count(*) as total_inserted,
        coalesce(sum(iff(market_type_group = 'STOCK', 1, 0)), 0) as stock_selected,
        coalesce(sum(iff(market_type_group = 'FX', 1, 0)), 0) as fx_selected,
        coalesce(sum(iff(market_type = 'ETF', 1, 0)), 0) as etf_selected
      into :v_inserted_count,
           :v_selected_stock,
           :v_selected_fx,
           :v_selected_etf
      from (
        select distinct
            s.RECOMMENDATION_ID,
            case
                when s.MARKET_TYPE = 'FX' then 'FX'
                else 'STOCK'
            end as market_type_group,
            s.MARKET_TYPE
        from MIP.AGENT_OUT.ORDER_PROPOSALS s
        where s.PORTFOLIO_ID = :P_PORTFOLIO_ID
          and s.RUN_ID_VARCHAR = :P_RUN_ID
          and s.STATUS = 'PROPOSED'
    ) selected_counts;

    v_selected_count := least(:v_remaining_capacity, :v_selected_stock + :v_selected_fx);

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        PARENT_RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS
    )
    select
        current_timestamp(),
        :v_run_id_string,
        :P_PARENT_RUN_ID,
        'AGENT',
        'SP_AGENT_PROPOSE_TRADES',
        'INFO',
        :v_inserted_count,
        object_construct(
            'max_positions', :v_max_positions,
            'open_positions', :v_open_positions,
            'remaining_capacity', :v_remaining_capacity,
            'candidate_count', :v_candidate_count,
            'proposed_count', :v_selected_count,
            'candidate_count_raw', :v_candidate_count_raw,
            'candidate_count_trusted', :v_candidate_count_trusted,
            'trusted_rejected_count', :v_trusted_rejected_count,
            'picked_by_market_type', object_construct(
                'STOCK', :v_selected_stock,
                'FX', :v_selected_fx,
                'ETF', :v_selected_etf
            ),
            'skipped_held_count', :v_skipped_held_count
        );

    -- Learning-to-Decision ledger append (non-fatal).
    begin
        call MIP.APP.SP_LEDGER_APPEND_EVENT(
            'DECISION_EVENT',
            'PROPOSAL_SELECTION',
            'SUCCESS',
            :P_RUN_ID,
            :P_PARENT_RUN_ID,
            :P_PORTFOLIO_ID,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            object_construct(
                'news_influence_enabled', (
                    select coalesce(max(lower(CONFIG_VALUE)), 'false')
                    from MIP.APP.APP_CONFIG
                    where CONFIG_KEY = 'NEWS_INFLUENCE_ENABLED'
                )
            ),
            object_construct(
                'candidate_count_raw', :v_candidate_count_raw,
                'candidate_count_trusted', :v_candidate_count_trusted,
                'remaining_capacity', :v_remaining_capacity
            ),
            object_construct(
                'proposal_selected', :v_selected_count,
                'proposal_inserted', :v_inserted_count,
                'picked_stock', :v_selected_stock,
                'picked_fx', :v_selected_fx
            ),
            object_construct(
                'eligibility_changed', iff(:v_candidate_count_raw != :v_candidate_count_trusted, true, false),
                'ranking_adjustment_active', true,
                'size_profile', object_construct(
                    'target_weight', :v_target_weight,
                    'max_position_pct', :v_max_position_pct
                ),
                'trusted_rejected_count', :v_trusted_rejected_count
            ),
            object_construct(
                'run_id', :P_RUN_ID,
                'portfolio_id', :P_PORTFOLIO_ID,
                'event_source', 'SP_AGENT_PROPOSE_TRADES'
            ),
            null,
            null
        );
    exception
        when other then null;
    end;

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID,
        'max_positions', :v_max_positions,
        'open_positions', :v_open_positions,
        'remaining_capacity', :v_remaining_capacity,
        'proposal_candidates', :v_candidate_count,
        'proposal_selected', :v_selected_count,
        'proposal_inserted', :v_inserted_count,
        'target_weight', :v_target_weight,
        'market_type_quota', object_construct(
            'STOCK', :v_max_new_stock,
            'FX', :v_max_new_fx
        ),
        'picked_by_market_type', object_construct(
            'STOCK', :v_selected_stock,
            'FX', :v_selected_fx,
            'ETF', :v_selected_etf
        ),
        'skipped_held_count', :v_skipped_held_count
    );
end;
$$;

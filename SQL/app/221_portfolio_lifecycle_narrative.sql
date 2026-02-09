-- 221_portfolio_lifecycle_narrative.sql
-- Purpose: AI-generated narrative for the portfolio lifecycle story.
-- Pattern: snapshot view -> Cortex prompt -> narrative table (same as training digest).
-- On-demand: called from the UI, NOT wired into the daily pipeline.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ═══════════════════════════════════════════════════════════════════════════════
-- TABLE: PORTFOLIO_LIFECYCLE_NARRATIVE
-- ═══════════════════════════════════════════════════════════════════════════════

create table if not exists MIP.AGENT_OUT.PORTFOLIO_LIFECYCLE_NARRATIVE (
    NARRATIVE_ID        number identity,
    PORTFOLIO_ID        number          not null,
    AS_OF_TS            timestamp_ntz   not null,
    RUN_ID              varchar(64)     not null,
    AGENT_NAME          varchar(128)    not null default 'PORTFOLIO_LIFECYCLE',
    NARRATIVE_TEXT       string,                  -- full prose narrative
    NARRATIVE_JSON       variant,                 -- structured: headline, chapters, outlook
    MODEL_INFO           varchar(256),
    SOURCE_FACTS_HASH    varchar(64),
    CREATED_AT           timestamp_ntz   default current_timestamp(),

    constraint PK_PORTFOLIO_LIFECYCLE_NARRATIVE primary key (NARRATIVE_ID),
    constraint UQ_PORTFOLIO_LIFECYCLE_NARRATIVE unique (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME)
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW: V_PORTFOLIO_LIFECYCLE_SNAPSHOT
-- ═══════════════════════════════════════════════════════════════════════════════
-- Computes the full lifecycle state as a single JSON object per portfolio.
-- This JSON is fed to the Cortex prompt for narrative generation.

create or replace view MIP.MART.V_PORTFOLIO_LIFECYCLE_SNAPSHOT as
with
-- Latest lifecycle running totals
lifecycle_latest as (
    select
        PORTFOLIO_ID,
        CUMULATIVE_DEPOSITED,
        CUMULATIVE_WITHDRAWN,
        CUMULATIVE_PNL,
        CASH_AFTER   as LATEST_CASH,
        EQUITY_AFTER as LATEST_EQUITY
    from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
    qualify row_number() over (partition by PORTFOLIO_ID order by EVENT_TS desc, EVENT_ID desc) = 1
),
-- All lifecycle events as array
lifecycle_events as (
    select
        PORTFOLIO_ID,
        array_agg(
            object_construct(
                'event_ts', to_varchar(EVENT_TS, 'YYYY-MM-DD HH24:MI'),
                'event_type', EVENT_TYPE,
                'amount', AMOUNT,
                'cash_after', CASH_AFTER,
                'equity_after', EQUITY_AFTER,
                'cumulative_pnl', CUMULATIVE_PNL,
                'notes', NOTES
            )
        ) within group (order by EVENT_TS, EVENT_ID) as EVENTS_ARRAY
    from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
    group by PORTFOLIO_ID
),
-- Episode summary
episode_summary as (
    select
        e.PORTFOLIO_ID,
        count(*)                                            as EPISODE_COUNT,
        count_if(e.STATUS = 'ACTIVE')                      as ACTIVE_EPISODES,
        count_if(e.STATUS = 'ENDED')                        as ENDED_EPISODES,
        avg(r.RETURN_PCT)                                   as AVG_EPISODE_RETURN,
        max(r.RETURN_PCT)                                   as BEST_EPISODE_RETURN,
        min(r.RETURN_PCT)                                   as WORST_EPISODE_RETURN,
        sum(r.DISTRIBUTION_AMOUNT)                          as TOTAL_DISTRIBUTIONS,
        sum(r.TRADES_COUNT)                                 as TOTAL_TRADES,
        array_agg(
            object_construct(
                'episode_id', e.EPISODE_ID,
                'profile_name', pp.NAME,
                'start_ts', to_varchar(e.START_TS, 'YYYY-MM-DD'),
                'end_ts', to_varchar(e.END_TS, 'YYYY-MM-DD'),
                'status', e.STATUS,
                'end_reason', e.END_REASON,
                'return_pct', r.RETURN_PCT,
                'max_drawdown_pct', r.MAX_DRAWDOWN_PCT,
                'trades_count', r.TRADES_COUNT,
                'distribution_amount', r.DISTRIBUTION_AMOUNT
            )
        ) within group (order by e.EPISODE_ID)              as EPISODES_ARRAY
    from MIP.APP.PORTFOLIO_EPISODE e
    left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
      on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
    left join MIP.APP.PORTFOLIO_PROFILE pp
      on pp.PROFILE_ID = e.PROFILE_ID
    group by e.PORTFOLIO_ID
),
-- Open positions count
open_positions as (
    select PORTFOLIO_ID, count(*) as OPEN_POSITION_COUNT
    from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
    where IS_OPEN
    group by PORTFOLIO_ID
)
select
    p.PORTFOLIO_ID,
    object_construct(
        'portfolio', object_construct(
            'portfolio_id', p.PORTFOLIO_ID,
            'name', p.NAME,
            'base_currency', p.BASE_CURRENCY,
            'starting_cash', p.STARTING_CASH,
            'final_equity', p.FINAL_EQUITY,
            'total_return', p.TOTAL_RETURN,
            'max_drawdown', p.MAX_DRAWDOWN,
            'status', p.STATUS,
            'created_at', to_varchar(p.CREATED_AT, 'YYYY-MM-DD'),
            'last_simulated_at', to_varchar(p.LAST_SIMULATED_AT, 'YYYY-MM-DD HH24:MI')
        ),
        'current_profile', object_construct(
            'profile_id', pp.PROFILE_ID,
            'name', pp.NAME,
            'max_positions', pp.MAX_POSITIONS,
            'crystallize_enabled', pp.CRYSTALLIZE_ENABLED,
            'profit_target_pct', pp.PROFIT_TARGET_PCT,
            'crystallize_mode', pp.CRYSTALLIZE_MODE
        ),
        'lifetime_stats', object_construct(
            'cumulative_deposited', coalesce(ll.CUMULATIVE_DEPOSITED, p.STARTING_CASH),
            'cumulative_withdrawn', coalesce(ll.CUMULATIVE_WITHDRAWN, 0),
            'net_contributed', coalesce(ll.CUMULATIVE_DEPOSITED, p.STARTING_CASH) - coalesce(ll.CUMULATIVE_WITHDRAWN, 0),
            'cumulative_pnl', coalesce(ll.CUMULATIVE_PNL, 0),
            'latest_cash', coalesce(ll.LATEST_CASH, p.STARTING_CASH),
            'latest_equity', coalesce(ll.LATEST_EQUITY, p.FINAL_EQUITY),
            'lifetime_return_pct', case
                when (coalesce(ll.CUMULATIVE_DEPOSITED, p.STARTING_CASH) - coalesce(ll.CUMULATIVE_WITHDRAWN, 0)) > 0
                then coalesce(ll.CUMULATIVE_PNL, 0) / (coalesce(ll.CUMULATIVE_DEPOSITED, p.STARTING_CASH) - coalesce(ll.CUMULATIVE_WITHDRAWN, 0))
                else 0
            end
        ),
        'episode_summary', object_construct(
            'episode_count', coalesce(es.EPISODE_COUNT, 0),
            'active_episodes', coalesce(es.ACTIVE_EPISODES, 0),
            'ended_episodes', coalesce(es.ENDED_EPISODES, 0),
            'avg_episode_return', es.AVG_EPISODE_RETURN,
            'best_episode_return', es.BEST_EPISODE_RETURN,
            'worst_episode_return', es.WORST_EPISODE_RETURN,
            'total_distributions', coalesce(es.TOTAL_DISTRIBUTIONS, 0),
            'total_trades', coalesce(es.TOTAL_TRADES, 0)
        ),
        'episodes', coalesce(es.EPISODES_ARRAY, array_construct()),
        'lifecycle_events', coalesce(le.EVENTS_ARRAY, array_construct()),
        'current_state', object_construct(
            'open_positions', coalesce(op.OPEN_POSITION_COUNT, 0),
            'days_since_creation', datediff(day, p.CREATED_AT, current_timestamp()),
            'days_since_last_sim', datediff(day, p.LAST_SIMULATED_AT, current_timestamp())
        )
    ) as SNAPSHOT_JSON
from MIP.APP.PORTFOLIO p
left join MIP.APP.PORTFOLIO_PROFILE pp on pp.PROFILE_ID = p.PROFILE_ID
left join lifecycle_latest ll on ll.PORTFOLIO_ID = p.PORTFOLIO_ID
left join lifecycle_events le on le.PORTFOLIO_ID = p.PORTFOLIO_ID
left join episode_summary es on es.PORTFOLIO_ID = p.PORTFOLIO_ID
left join open_positions op on op.PORTFOLIO_ID = p.PORTFOLIO_ID;


-- ═══════════════════════════════════════════════════════════════════════════════
-- SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE
-- ═══════════════════════════════════════════════════════════════════════════════
-- On-demand: called from the UI when the user clicks "Generate Story".
-- Follows the exact same pattern as SP_AGENT_GENERATE_TRAINING_DIGEST.

create or replace procedure MIP.APP.SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE(
    P_PORTFOLIO_ID  number,
    P_RUN_ID        varchar default null,
    P_AS_OF_TS      timestamp_ntz default null
)
returns variant
language sql
execute as owner
as
$$
declare
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_run_id varchar := coalesce(:P_RUN_ID, uuid_string());
    v_as_of_ts timestamp_ntz := coalesce(:P_AS_OF_TS, current_timestamp());
    v_agent_name varchar := 'PORTFOLIO_LIFECYCLE';
    v_model_name varchar := 'mistral-large2';
    v_snapshot variant;
    v_facts_hash varchar;
    v_narrative_text string;
    v_narrative_json variant;
    v_cortex_prompt string;
    v_cortex_succeeded boolean := false;
    v_portfolio_name varchar;
begin
    -- Load snapshot
    select SNAPSHOT_JSON
      into :v_snapshot
      from MIP.MART.V_PORTFOLIO_LIFECYCLE_SNAPSHOT
     where PORTFOLIO_ID = :v_portfolio_id
     limit 1;

    if (v_snapshot is null) then
        return object_construct('status', 'ERROR', 'error', 'Portfolio not found or no snapshot available.');
    end if;

    v_facts_hash := (select sha2(to_varchar(:v_snapshot), 256));
    v_portfolio_name := :v_snapshot:portfolio:name::string;

    -- Build the Cortex prompt
    v_cortex_prompt :=
'You are a portfolio analyst and narrator for MIP (Market Intelligence Platform). ' ||
'Your job is to write a compelling, flowing narrative about a portfolio''s entire lifecycle — its story from creation to today. ' ||
'This narrative will be read by BOTH the trader who manages the portfolio AND a customer/investor who wants to understand how their money is doing. ' ||
'You MUST only reference numbers and facts present in the snapshot data below. ' ||
'Do NOT invent facts, propose trades, or make recommendations. ' ||
'
PORTFOLIO LIFECYCLE SNAPSHOT:
' || to_varchar(:v_snapshot) || '

Write a narrative that tells the COMPLETE STORY of this portfolio. The output must be a JSON object with these keys:

{
  "headline": "One compelling sentence capturing the portfolio''s current state and lifetime journey",
  "narrative": "The full prose narrative (3-6 paragraphs, see rules below)",
  "key_moments": ["moment 1 description", "moment 2 description", ...],
  "outlook": "1-2 sentences on what the current state means going forward"
}

NARRATIVE STRUCTURE (must follow this flow):

1. OPENING PARAGRAPH — The Birth:
   Start with when the portfolio was created, with how much starting cash, and under which risk profile.
   Set the scene: "This portfolio began its journey on [date] with $[amount], configured with the [profile] risk profile..."
   If there have been deposits or withdrawals, mention the total capital committed.

2. THE JOURNEY — Episodes and Events (1-3 paragraphs):
   Walk through the major lifecycle events chronologically. This is the meat of the story.
   - For each episode: what profile was active, how did it perform (return, drawdown), how many trades, how did it end?
   - For crystallizations: explain that profits were locked in, how much was distributed, and what mode was used.
   - For deposits/withdrawals: explain the cash flow and its impact on the portfolio.
   - For profile changes: explain the shift and what it means for risk posture.
   - Connect events to each other: "After the first episode returned 8.3%, profits of $830 were crystallized and withdrawn..."
   Use specific numbers, dates, and percentages. Make the numbers human-readable (e.g., "8.3%" not "0.083").

3. WHERE WE STAND TODAY (1 paragraph):
   Current equity, current cash, open positions, lifetime P&L.
   Compare current state to starting point: "From an initial investment of $10,000 plus a $5,000 deposit,
   the portfolio now stands at $16,200 — a lifetime gain of $1,200 or 8.0%."
   Mention the current risk profile and what it means.

4. THE NUMBERS THAT MATTER (woven into narrative, not as a separate list):
   - Lifetime P&L (absolute and percentage)
   - Total deposited vs. total withdrawn
   - Number of episodes completed
   - Average episode return
   - Total distributions paid out

CRITICAL RULES:
- Write in CONNECTED PARAGRAPHS. This is a narrative, not bullet points.
- Write as if composing a quarterly letter from a fund manager to investors.
- Every number you mention MUST appear in the snapshot data. Do NOT invent.
- Convert all decimals to percentages: 0.083 → "8.3%", 0.005 → "0.5%".
- Format dollar amounts with commas: $10,000 not $10000.
- The tone should be professional yet accessible — informative with personality.
- If the portfolio is young (few events), say so honestly: "Still in early days..."
- If things are going badly (losses, bust), be straightforward but constructive.
- The "key_moments" array should list 3-6 defining events in the portfolio''s life.
- Return ONLY the raw JSON object. Start with { and end with }.';

    -- Call Cortex
    begin
        v_narrative_text := (select snowflake.cortex.complete(:v_model_name, :v_cortex_prompt));
        v_cortex_succeeded := true;
    exception
        when other then
            v_cortex_succeeded := false;
            call MIP.APP.SP_LOG_EVENT('AGENT', 'SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE',
                'WARN_CORTEX_FAILED', null,
                object_construct('portfolio_id', :v_portfolio_id, 'run_id', :v_run_id, 'error', :sqlerrm),
                :sqlerrm, :v_run_id, null);
    end;

    -- Parse or fallback
    if (:v_cortex_succeeded and :v_narrative_text is not null) then
        v_narrative_text := trim(:v_narrative_text);
        -- Strip markdown fences if present
        if (left(:v_narrative_text, 7) = '```json') then
            v_narrative_text := trim(substr(:v_narrative_text, 8));
        elseif (left(:v_narrative_text, 3) = '```') then
            v_narrative_text := trim(substr(:v_narrative_text, 4));
        end if;
        if (right(:v_narrative_text, 3) = '```') then
            v_narrative_text := trim(substr(:v_narrative_text, 1, length(:v_narrative_text) - 3));
        end if;

        begin
            v_narrative_json := parse_json(:v_narrative_text);
        exception
            when other then
                v_narrative_json := object_construct(
                    'headline', 'Portfolio narrative for ' || coalesce(:v_portfolio_name, 'Portfolio ' || :v_portfolio_id),
                    'narrative', :v_narrative_text,
                    'key_moments', array_construct('Narrative generated but JSON parsing failed — raw text preserved.'),
                    'outlook', 'See narrative text above.'
                );
        end;
    else
        -- Deterministic fallback narrative
        v_narrative_text := 'Portfolio ' || coalesce(:v_portfolio_name, to_varchar(:v_portfolio_id)) ||
            ' was created with $' || to_varchar(:v_snapshot:portfolio:starting_cash::number, '999,999,999.00') ||
            '. Current equity stands at $' || to_varchar(coalesce(:v_snapshot:portfolio:final_equity::number, :v_snapshot:portfolio:starting_cash::number), '999,999,999.00') ||
            '. ' || to_varchar(coalesce(:v_snapshot:episode_summary:episode_count::number, 0)) || ' episodes have been recorded.' ||
            ' Lifetime P&L: $' || to_varchar(coalesce(:v_snapshot:lifetime_stats:cumulative_pnl::number, 0), '999,999,999.00') || '.';

        v_narrative_json := object_construct(
            'headline', 'Portfolio summary for ' || coalesce(:v_portfolio_name, 'Portfolio ' || :v_portfolio_id),
            'narrative', :v_narrative_text,
            'key_moments', array_construct('Portfolio created', 'Current state captured'),
            'outlook', 'AI narrative generation was unavailable. This is a deterministic fallback summary.'
        );
    end if;

    -- MERGE into narrative table (idempotent)
    merge into MIP.AGENT_OUT.PORTFOLIO_LIFECYCLE_NARRATIVE as target
    using (
        select
            :v_portfolio_id::number             as PORTFOLIO_ID,
            :v_as_of_ts::timestamp_ntz          as AS_OF_TS,
            :v_run_id::varchar                  as RUN_ID,
            :v_agent_name::varchar              as AGENT_NAME,
            :v_narrative_text::string           as NARRATIVE_TEXT,
            :v_narrative_json::variant          as NARRATIVE_JSON,
            :v_model_name::varchar              as MODEL_INFO,
            :v_facts_hash::varchar              as SOURCE_FACTS_HASH
    ) as source
    on  target.PORTFOLIO_ID = source.PORTFOLIO_ID
    and target.AS_OF_TS     = source.AS_OF_TS
    and target.RUN_ID       = source.RUN_ID
    and target.AGENT_NAME   = source.AGENT_NAME
    when matched then update set
        NARRATIVE_TEXT    = source.NARRATIVE_TEXT,
        NARRATIVE_JSON   = source.NARRATIVE_JSON,
        MODEL_INFO       = source.MODEL_INFO,
        SOURCE_FACTS_HASH = source.SOURCE_FACTS_HASH
    when not matched then insert (
        PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME,
        NARRATIVE_TEXT, NARRATIVE_JSON, MODEL_INFO, SOURCE_FACTS_HASH, CREATED_AT
    ) values (
        source.PORTFOLIO_ID, source.AS_OF_TS, source.RUN_ID, source.AGENT_NAME,
        source.NARRATIVE_TEXT, source.NARRATIVE_JSON, source.MODEL_INFO,
        source.SOURCE_FACTS_HASH, current_timestamp()
    );

    return object_construct(
        'status', 'SUCCESS',
        'portfolio_id', :v_portfolio_id,
        'cortex_succeeded', :v_cortex_succeeded,
        'model_info', :v_model_name,
        'facts_hash', :v_facts_hash
    );

exception
    when other then
        return object_construct(
            'status', 'ERROR',
            'portfolio_id', :v_portfolio_id,
            'error', :sqlerrm
        );
end;
$$;

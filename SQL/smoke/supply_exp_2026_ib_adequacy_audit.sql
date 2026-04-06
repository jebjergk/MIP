-- supply_exp_2026_ib_adequacy_audit.sql
-- Formal IB adequacy vs plan: D_HISTORY_FLOOR, D_AS_OF, NYSE session set U from SPY (IBKR daily).
-- Run as CURSOR_AGENT / MIP_ADMIN_ROLE.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Reference calendar: SPY daily IBKR
with spy as (
    select distinct b.TS::date as d
    from MIP.MART.MARKET_BARS b
    where upper(b.SYMBOL) = 'SPY'
      and b.INTERVAL_MINUTES = 1440
      and upper(coalesce(b.SOURCE, '')) = 'IBKR'
),
params as (
    select
        (select min(d) from spy where d >= '2025-08-01') as d_phase_start,
        (select max(d) from spy) as d_as_of
),
d_hist as (
    select s.d as d_history_floor
    from spy s
    cross join params p
    where s.d < p.d_phase_start
    qualify row_number() over (order by s.d desc) = 60
),
u_days as (
    select s.d
    from spy s
    cross join d_hist h
    cross join params p
    where s.d between h.d_history_floor and p.d_as_of
),
cohort as (
    select upper(iu.SYMBOL) as sym
    from MIP.APP.INGEST_UNIVERSE iu
    where iu.SYMBOL_COHORT = 'SUPPLY_EXP_2026'
      and upper(iu.MARKET_TYPE) = 'STOCK'
      and iu.INTERVAL_MINUTES = 1440
),
sym_bar_days as (
    select
        upper(b.SYMBOL) as sym,
        b.TS::date as d,
        count(*) as bar_rows
    from MIP.MART.MARKET_BARS b
    inner join cohort c on upper(b.SYMBOL) = c.sym
    where b.INTERVAL_MINUTES = 1440
      and upper(coalesce(b.SOURCE, '')) = 'IBKR'
    group by 1, 2
),
first_last as (
    select
        sym,
        min(d) as first_bar_date,
        max(d) as last_bar_date,
        count(distinct d) as distinct_bar_days
    from sym_bar_days
    group by sym
),
missing as (
    select c.sym, u.d as gap_date
    from cohort c
    cross join u_days u
    left join sym_bar_days s
      on s.sym = c.sym and s.d = u.d
    where s.d is null
),
missing_agg as (
    select sym, min(gap_date) as first_gap_date, count(*) as gap_count
    from missing
    group by sym
),
audit as (
    select
        c.sym as symbol,
        p.d_phase_start,
        p.d_as_of,
        h.d_history_floor,
        fl.first_bar_date,
        fl.last_bar_date,
        fl.distinct_bar_days,
        (select count(*) from u_days) as expected_session_days,
        ma.first_gap_date,
        ma.gap_count,
        case
            when fl.first_bar_date is null then 'IB_INADEQUATE'
            when fl.first_bar_date > h.d_history_floor then 'INSUFFICIENT_HISTORY'
            when fl.last_bar_date < p.d_as_of then 'IB_INADEQUATE'
            when ma.gap_count is not null and ma.gap_count > 0 then 'CALENDAR_GAP'
            else 'PASS'
        end as status,
        case
            when fl.first_bar_date is null then 'NO_IBKR_BARS'
            when fl.first_bar_date > h.d_history_floor then 'FIRST_BAR_AFTER_D_HISTORY_FLOOR'
            when fl.last_bar_date < p.d_as_of then 'LAST_BAR_BEFORE_D_AS_OF'
            when ma.gap_count > 0 then 'MISSING_NYSE_SESSION'
            else null
        end as reason_detail
    from cohort c
    cross join params p
    cross join d_hist h
    left join first_last fl on fl.sym = c.sym
    left join missing_agg ma on ma.sym = c.sym
)
select * from audit order by symbol;

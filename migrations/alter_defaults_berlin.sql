-- alter_defaults_berlin.sql
-- Purpose: Standardize default system timestamps to Berlin NTZ clock

use role MIP_ADMIN_ROLE;
use database MIP;

-- MIP.APP tables
alter table MIP.APP.INGEST_UNIVERSE
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.PATTERN_DEFINITION
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.RECOMMENDATION_LOG
    alter column GENERATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.RECOMMENDATION_OUTCOMES
    alter column CALCULATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.BACKTEST_RUN
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.MIP_AUDIT_LOG
    alter column EVENT_TS set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.PORTFOLIO_PROFILE
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.PORTFOLIO
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.PORTFOLIO_POSITIONS
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.PORTFOLIO_TRADES
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

alter table MIP.APP.PORTFOLIO_DAILY
    alter column CREATED_AT set default MIP.APP.F_NOW_BERLIN_NTZ();

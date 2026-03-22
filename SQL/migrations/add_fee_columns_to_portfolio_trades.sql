-- Migration: Add fee tracking columns to PORTFOLIO_TRADES
-- Purpose: Make fee accounting explicit and inspectable.
-- COMMISSION = broker commission (actual or estimated)
-- REGULATORY_FEE = SEC/TAF fees (future-ready)
-- FX_CONVERSION_COST = EUR/USD conversion cost (future-ready)
-- TOTAL_FEE = sum of all fee components (convenience)
-- FEE_SOURCE = provenance: ESTIMATED | ACTUAL_BROKER | DERIVED_BACKFILL

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.APP.PORTFOLIO_TRADES add column if not exists COMMISSION number(18,8) default 0;
alter table MIP.APP.PORTFOLIO_TRADES add column if not exists REGULATORY_FEE number(18,8) default 0;
alter table MIP.APP.PORTFOLIO_TRADES add column if not exists FX_CONVERSION_COST number(18,8) default 0;
alter table MIP.APP.PORTFOLIO_TRADES add column if not exists TOTAL_FEE number(18,8) default 0;
alter table MIP.APP.PORTFOLIO_TRADES add column if not exists FEE_SOURCE varchar(20) default 'ESTIMATED';

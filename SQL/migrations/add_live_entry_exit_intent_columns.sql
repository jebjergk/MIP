use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists ACTION_INTENT string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists EXIT_TYPE string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists EXIT_REASON string;

alter table MIP.LIVE.LIVE_ORDERS
    add column if not exists ACTION_INTENT string;

alter table MIP.LIVE.LIVE_ORDERS
    add column if not exists EXIT_TYPE string;

update MIP.LIVE.LIVE_ACTIONS
set ACTION_INTENT = case
    when upper(coalesce(SIDE, '')) = 'SELL' then 'EXIT'
    else 'ENTRY'
end
where ACTION_INTENT is null;

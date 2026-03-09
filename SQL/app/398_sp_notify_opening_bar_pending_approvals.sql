-- 398_sp_notify_opening_bar_pending_approvals.sql
-- Purpose: Email pending approvals (or no pending approvals) during opening-bar flow.
-- Behavior: Sends only when state changes, to avoid 5-minute spam loops.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_NOTIFY_OPENING_BAR_PENDING_APPROVALS(
    P_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_enabled boolean := false;
    v_recipients string;
    v_email_integration string;
    v_last_signature string;
    v_now_ny timestamp_tz := convert_timezone('America/New_York', current_timestamp());
    v_pending_count number := 0;
    v_state_signature string;
    v_subject string;
    v_body string;
    v_email_result variant;
begin
    -- Load config
    v_enabled := coalesce(try_to_boolean((
        select CONFIG_VALUE
          from MIP.APP.APP_CONFIG
         where CONFIG_KEY = 'OPENING_BAR_EMAIL_NOTIFY_ENABLED'
    )), false);

    v_recipients := trim(coalesce((
        select CONFIG_VALUE
          from MIP.APP.APP_CONFIG
         where CONFIG_KEY = 'OPENING_BAR_EMAIL_RECIPIENTS'
    ), ''));

    v_email_integration := trim(coalesce((
        select CONFIG_VALUE
          from MIP.APP.APP_CONFIG
         where CONFIG_KEY = 'OPENING_BAR_EMAIL_INTEGRATION'
    ), ''));

    v_last_signature := coalesce((
        select CONFIG_VALUE
          from MIP.APP.APP_CONFIG
         where CONFIG_KEY = 'OPENING_BAR_LAST_NOTIFY_SIGNATURE'
    ), '');

    if (not v_enabled) then
        return object_construct('status', 'SKIPPED', 'reason', 'NOTIFY_DISABLED');
    end if;

    if (v_recipients = '' or v_email_integration = '') then
        return object_construct(
            'status', 'SKIPPED',
            'reason', 'MISSING_EMAIL_CONFIG',
            'integration', v_email_integration,
            'recipients', v_recipients
        );
    end if;

    select count(*)
      into :v_pending_count
      from MIP.AGENT_OUT.ORDER_PROPOSALS p
     where p.STATUS = 'PROPOSED'
       and (P_RUN_ID is null or p.RUN_ID_VARCHAR = P_RUN_ID)
       and p.PROPOSED_AT >= dateadd(day, -1, current_timestamp());

    if (v_pending_count > 0) then
        select sha2(
                   listagg(
                       coalesce(to_varchar(p.PROPOSAL_ID), '') || '|' ||
                       coalesce(to_varchar(p.PORTFOLIO_ID), '') || '|' ||
                       coalesce(p.RUN_ID_VARCHAR, '') || '|' ||
                       coalesce(p.SYMBOL, '') || '|' ||
                       coalesce(p.SIDE, '') || '|' ||
                       coalesce(to_varchar(p.TARGET_WEIGHT), '') || '|' ||
                       coalesce(to_varchar(p.PROPOSED_AT), ''),
                       ';'
                   ) within group (order by p.PROPOSED_AT, p.PROPOSAL_ID),
                   256
               )
          into :v_state_signature
          from MIP.AGENT_OUT.ORDER_PROPOSALS p
         where p.STATUS = 'PROPOSED'
           and (P_RUN_ID is null or p.RUN_ID_VARCHAR = P_RUN_ID)
           and p.PROPOSED_AT >= dateadd(day, -1, current_timestamp());
    else
        v_state_signature := 'NONE';
    end if;

    if (coalesce(v_last_signature, '') = coalesce(v_state_signature, '')) then
        return object_construct(
            'status', 'SKIPPED',
            'reason', 'NO_STATE_CHANGE',
            'pending_count', v_pending_count,
            'state_signature', v_state_signature
        );
    end if;

    if (v_pending_count > 0) then
        select
            'MIP Opening Bar: ' || to_varchar(:v_pending_count) || ' pending approval(s) ready'
          into :v_subject;

        select
            'Opening-bar decision update (' || to_varchar(:v_now_ny, 'YYYY-MM-DD HH24:MI:SS TZH:TZM') || ')' || char(10) || char(10) ||
            'Pending approvals requiring action: ' || to_varchar(:v_pending_count) || char(10) || char(10) ||
            listagg(
                '- Portfolio ' || to_varchar(p.PORTFOLIO_ID) ||
                ' | ' || coalesce(p.SYMBOL, '?') ||
                ' | ' || coalesce(p.SIDE, '?') ||
                ' | run ' || coalesce(p.RUN_ID_VARCHAR, '?') ||
                ' | proposed ' || to_varchar(p.PROPOSED_AT, 'YYYY-MM-DD HH24:MI:SS'),
                char(10)
            ) within group (order by p.PROPOSED_AT, p.PROPOSAL_ID)
          into :v_body
          from MIP.AGENT_OUT.ORDER_PROPOSALS p
         where p.STATUS = 'PROPOSED'
           and (P_RUN_ID is null or p.RUN_ID_VARCHAR = P_RUN_ID)
           and p.PROPOSED_AT >= dateadd(day, -1, current_timestamp());
    else
        v_subject := 'MIP Opening Bar: no pending approvals';
        v_body := 'Opening-bar decision update (' || to_varchar(:v_now_ny, 'YYYY-MM-DD HH24:MI:SS TZH:TZM') || ')' || char(10) || char(10) ||
                  'There are currently no pending approvals waiting for action.';
    end if;

    select system$send_email(
               :v_email_integration,
               :v_recipients,
               :v_subject,
               :v_body,
               'text/plain'
           )
      into :v_email_result;

    merge into MIP.APP.APP_CONFIG t
    using (
        select 'OPENING_BAR_LAST_NOTIFY_SIGNATURE' as CONFIG_KEY,
               :v_state_signature as CONFIG_VALUE,
               'Internal dedupe token for opening-bar pending-approval emails' as DESCRIPTION
    ) s
    on t.CONFIG_KEY = s.CONFIG_KEY
    when matched then update set
        t.CONFIG_VALUE = s.CONFIG_VALUE,
        t.DESCRIPTION = s.DESCRIPTION,
        t.UPDATED_AT = current_timestamp()
    when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
    values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);

    return object_construct(
        'status', 'SENT',
        'pending_count', v_pending_count,
        'subject', v_subject,
        'recipients', v_recipients,
        'integration', v_email_integration,
        'state_signature', v_state_signature,
        'email_result', v_email_result
    );
end;
$$;

grant usage on procedure MIP.APP.SP_NOTIFY_OPENING_BAR_PENDING_APPROVALS(string) to role MIP_UI_API_ROLE;
grant usage on procedure MIP.APP.SP_NOTIFY_OPENING_BAR_PENDING_APPROVALS(string) to role MIP_APP_ROLE;

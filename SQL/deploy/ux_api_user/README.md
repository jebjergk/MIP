# UX API User Deployment Kit

Idempotent SQL scripts to create and configure the MIP UX API service user (read-only access for the external FastAPI app).

## Prerequisites

- MIP bootstrap applied (`MIP/SQL/bootstrap/001_bootstrap_mip_infra.sql`)
- `SECURITYADMIN` and `MIP_ADMIN_ROLE` access

## Run order

1. **01_create_role_and_user.sql** — Create role and user
2. **02_grants_readonly.sql** — Grant read-only access (MIP.APP, MIP.MART, MIP.AGENT_OUT)
3. **03_set_rsa_public_key.sql** — Set RSA public key for keypair auth (MFA environments)

## Placeholders

Snowflake worksheets don't support bind variables. Before running, replace placeholders in each file:

| Placeholder       | Default          | Description                    |
|-------------------|------------------|--------------------------------|
| `:ux_user`        | MIP_UI_API       | Service user name              |
| `:ux_role`        | MIP_UI_API_ROLE  | Role name                      |
| `:warehouse_name` | MIP_WH_XS        | Warehouse for queries          |
| `:rsa_public_key` | (required)       | Public key body, no header/footer |

Scripts use defaults where noted; edit if you need different names.

## Rollback

**99_drop_ux_api_user.sql** — Drops user and role. Run only for rollback.

## Env vars (API)

After deployment, configure the MIP UI API `.env`:

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=MIP_UI_API
SNOWFLAKE_AUTH_METHOD=keypair
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8
SNOWFLAKE_ROLE=MIP_UI_API_ROLE
SNOWFLAKE_WAREHOUSE=MIP_WH_XS
SNOWFLAKE_DATABASE=MIP
SNOWFLAKE_SCHEMA=APP
```

See [docs/73_UX_RUNBOOK.md](../../../docs/73_UX_RUNBOOK.md) for keypair generation.

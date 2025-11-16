# Market Intelligence Platform

## Infra & Security Model

### Snowflake-Native Overview
The Market Intelligence Platform (MIP) runs entirely within Snowflake, leveraging Snowflake-native data, compute, and governance features to keep application logic and analytics close to the data.

### Database
- **MIP** – Primary database housing all schemas, warehouses, and runtime objects for the platform.

### Schemas
- **MIP.RAW_EXT** – Raw and marketplace-sourced external data (e.g., TraderMade) retained in its original grain.
- **MIP.MART** – Curated analytic models, reporting views, and downstream-ready table functions.
- **MIP.APP** – Application configuration tables, Streamlit support artifacts, and future shared stored procedures.
- **MIP.AGENT_OUT** – Reserved for later agentic AI outputs and evaluation artifacts.

### Warehouses
- **MIP_WH_XS** – Primary extra-small warehouse for cost-efficient runtime and Streamlit workloads.
- **MIP_WH_S** – Optional larger warehouse used when heavier processing or backfills are required.

### Roles
- **MIP_ADMIN_ROLE** – Full administrative and deployment control over all MIP assets.
- **MIP_APP_ROLE** – Least-privilege runtime access for the application surface area.
- **MIP_AGENT_READ_ROLE** – Read-only access scoped to agent-generated outputs and supporting context.

### Security Model
- Admin role manages grants, warehouse operations, and elevated maintenance; app role executes production workloads; agent read role consumes curated outputs without modification rights.
- Imported privileges are granted on **PUBLIC_DOMAIN_DATA** and **TRADERMADE_CURRENCY_EXCHANGE_RATES** to expose shared marketplace datasets while preserving central governance.

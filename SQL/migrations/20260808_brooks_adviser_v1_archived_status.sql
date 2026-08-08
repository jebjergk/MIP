-- Brooks INTRADAY Adviser V1.0: ARCHIVED attempt status (data retained, UI-hidden).
-- STATUS values: RUNNING | COMPLETED | FAILED | ARCHIVED
-- Reviewable UI + re-validation guard use STATUS = 'COMPLETED' only.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

COMMENT ON COLUMN MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT.STATUS IS
  'RUNNING | COMPLETED | FAILED | ARCHIVED. ARCHIVED keeps call/bar/trade rows but excludes attempt from V1 Learning View and frozen validation re-run block.';

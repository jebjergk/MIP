-- News diagnostic: APP_CONFIG flags (run SHOW TASKS separately in a worksheet)
select CONFIG_KEY, CONFIG_VALUE
from MIP.APP.APP_CONFIG
where CONFIG_KEY like 'NEWS%'
order by CONFIG_KEY;

-- show tasks in schema MIP.NEWS;

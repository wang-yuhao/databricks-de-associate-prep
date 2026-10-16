-- SQL works in the OSS engine too: a materialized view over a Python-defined table.
CREATE MATERIALIZED VIEW gold_daily_status AS
SELECT event_date, status, count(*) AS events, round(sum(amount), 2) AS total_amount
FROM silver_events
GROUP BY event_date, status;

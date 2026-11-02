-- Staging: bronze raw JSON rows, typed. No business logic here.
-- dbt equivalent: models/staging/stg_events.sql (materialized as a view).
CREATE TABLE stg_events AS
SELECT
    event_id,
    event_time,
    received_at,
    order_id,
    customer_id,
    product_category,
    channel,
    status,
    CAST(amount AS REAL) AS amount,
    currency
FROM bronze_events;

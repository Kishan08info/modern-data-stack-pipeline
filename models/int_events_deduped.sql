-- Intermediate: one row per event_id (latest delivery wins), bad rows
-- quarantined with a reason instead of silently dropped.
-- dbt equivalent: an intermediate model + a quarantine model.
CREATE TABLE quarantine_events AS
SELECT *, CASE
    WHEN order_id IS NULL THEN 'missing_order_id'
    WHEN amount IS NULL THEN 'null_amount'
    WHEN amount < 0 THEN 'negative_amount'
    WHEN status NOT IN ('placed', 'paid', 'cancelled', 'refunded')
        THEN 'unknown_status'
END AS quarantine_reason
FROM stg_events
WHERE order_id IS NULL
   OR amount IS NULL
   OR amount < 0
   OR status NOT IN ('placed', 'paid', 'cancelled', 'refunded');

CREATE TABLE int_events_deduped AS
SELECT event_id, event_time, received_at, order_id, customer_id,
       product_category, channel, status, amount, currency
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id ORDER BY received_at DESC
           ) AS rn
    FROM stg_events
    WHERE event_id NOT IN (SELECT event_id FROM quarantine_events)
) WHERE rn = 1;

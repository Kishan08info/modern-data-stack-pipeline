-- Gold marts. fct_orders keeps the LATEST event per order (event_time,
-- then received_at), so a paid-then-refunded order ends as refunded and a
-- late-arriving correction overwrites the earlier state.
-- dbt equivalent: models/marts/fct_orders.sql + mart_*.sql.
CREATE TABLE dim_customers AS
SELECT customer_id,
       MIN(DATE(event_time)) AS first_seen_date,
       COUNT(DISTINCT order_id) AS orders_seen
FROM int_events_deduped
GROUP BY customer_id;

CREATE TABLE fct_orders AS
SELECT order_id, customer_id, product_category, channel, status, amount,
       DATE(event_time) AS event_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY event_time DESC, received_at DESC
           ) AS rn
    FROM int_events_deduped
) WHERE rn = 1;

CREATE TABLE mart_daily_revenue AS
SELECT event_date,
       COUNT(*) AS orders,
       SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS paid_orders,
       ROUND(SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END), 2)
           AS revenue,
       ROUND(SUM(CASE WHEN status = 'refunded' THEN amount ELSE 0 END), 2)
           AS refunded_amount
FROM fct_orders
GROUP BY event_date
ORDER BY event_date;

CREATE TABLE mart_revenue_by_category AS
SELECT product_category,
       COUNT(*) AS orders,
       SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS paid_orders,
       ROUND(SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END), 2)
           AS revenue
FROM fct_orders
GROUP BY product_category
ORDER BY revenue DESC;

CREATE TABLE mart_orders_by_channel AS
SELECT channel,
       COUNT(*) AS orders,
       SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS paid_orders,
       ROUND(100.0 * SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END)
             / COUNT(*), 2) AS paid_rate_pct
FROM fct_orders
GROUP BY channel
ORDER BY orders DESC;

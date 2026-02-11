WITH customer_orders AS (
    SELECT
        customer_id,
        MIN(created_at) AS first_order_date,
        COUNT(*) AS number_of_orders,
        SUM(amount_usd) AS lifetime_value
    FROM {{ ref('stg_orders') }}
    WHERE status = 'completed'
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    o.first_order_date AS first_order_date,
    COALESCE(o.number_of_orders, 0) AS number_of_orders,
    COALESCE(o.lifetime_value, 0) AS lifetime_value,
    CASE
        WHEN COALESCE(o.lifetime_value, 0) > 100 THEN 'VIP'
        WHEN COALESCE(o.lifetime_value, 0) > 50 THEN 'High Value'
        ELSE 'Standard'
    END AS customer_segment
FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_orders o
  ON c.customer_id = o.customer_id

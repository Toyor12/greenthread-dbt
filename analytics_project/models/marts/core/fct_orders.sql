SELECT
    o.order_id,
    o.customer_id,
    o.status,
    o.amount_usd,
    o.created_at,
    CASE
        WHEN o.status = 'completed' AND o.amount_usd = 0 THEN TRUE
        ELSE FALSE
    END AS is_revenue_leak
FROM {{ ref('stg_orders') }} o

SELECT
    id AS order_id,
    customer_id,
    status,
    amount_cents / 100.0 AS amount_usd,
    created_at,
    updated_at
FROM {{ source('raw', 'orders') }}

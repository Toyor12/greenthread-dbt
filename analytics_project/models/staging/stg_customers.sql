SELECT
    id AS customer_id,
    first_name,
    last_name,
    LOWER(TRIM(email)) AS email,
    country,
    created_at,
    updated_at
FROM {{ source('raw', 'customers') }}

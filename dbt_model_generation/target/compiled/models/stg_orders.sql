

WITH source_data AS (
    SELECT 
        id,
        store_id,
        customer,
        subtotal,
        tax_paid,
        order_total,
        ordered_at
    FROM demo_db.raw.raw_orders
),

transformed AS (
    SELECT 
        -- Rename columns as per target mapping
        id::TEXT AS order_id,
        store_id::TEXT AS location_id,
        customer::TEXT AS customer_id,
        
        -- Keep original cent values
        subtotal::DECIMAL AS subtotal_cents,
        tax_paid::DECIMAL AS tax_paid_cents,
        order_total::DECIMAL AS order_total_cents,
        
        -- Convert cents to dollars
        (subtotal / 100.0)::DECIMAL(10,2) AS subtotal,
        (tax_paid / 100.0)::DECIMAL(10,2) AS tax_paid,
        (order_total / 100.0)::DECIMAL(10,2) AS order_total,
        
        -- Extract date from timestamp
        ordered_at::DATE AS ordered_at
        
    FROM source_data
)

SELECT * FROM transformed
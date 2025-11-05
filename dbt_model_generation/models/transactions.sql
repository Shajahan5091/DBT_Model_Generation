/*
 * Model Name: stg_transactions
 * Description: Staging model for transactions with GST calculation
 * Created Date: 2024-12-19
 * Author: Shajahan
 */

WITH source_data AS (
    SELECT 
        transaction_id,
        amount
    FROM {{ source('raw', 'raw_transactions') }}
),

transformed AS (
    SELECT 
        -- Direct mapping for transaction_id
        CAST(transaction_id AS STRING) AS transaction_id,
        
        -- Add 18% GST to amount
        CAST(amount * 1.18 AS FLOAT) AS amount_with_tax
        
    FROM source_data
)

SELECT * FROM transformed
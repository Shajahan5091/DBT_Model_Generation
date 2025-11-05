/*
  Model: stg_transactions
  Description: Staging model for transactions data with GST calculations
  Created Date: 2024-12-19
  Author: AI Generated
*/

WITH source_data AS (
    SELECT 
        transaction_id,
        amount
    FROM {{ source('raw', 'raw_transactions') }}
),

transformed AS (
    SELECT 
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(amount * 1.18 AS FLOAT) AS amount_with_tax
    FROM source_data
)

SELECT * FROM transformed
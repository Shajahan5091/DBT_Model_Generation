{{ config(materialized='incremental') }}

WITH source_data AS (
    SELECT
    {% for col in columns %}
        {{ col.transformation_logic or col.source_column }} AS {{ col.target_column }}{% if not loop.last %},{% endif %}
    {% endfor %}
    FROM {{ source_table }}
)

SELECT * FROM source_data

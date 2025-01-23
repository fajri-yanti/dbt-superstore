-- dim_address.sql
{{
  config(
    materialized='table'
  )
}}

WITH address_data AS (
    SELECT DISTINCT 
        `Country` AS country,
        `State` AS state,
        `Region` AS region,
        `City` AS city,
        `Postal Code` AS postal_code
    FROM
        {{ source('bronze', 'transaction_superstore') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key([
        'country', 
        'state',
        'city',
        'postal_code'
    ])}} AS address_key,
    country,
    state,
    region,
    city,
    postal_code
FROM address_data


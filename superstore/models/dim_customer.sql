{{
  config(
    materialized='table'
  )
}}

WITH customer_address AS (
    SELECT DISTINCT 
        `Customer ID` AS customer_id,
        `Customer Name` AS customer_name,
        `Segment` AS segment,
        addr.address_key 
    FROM 
        {{ source('bronze', 'transaction_superstore') }} cust
        LEFT JOIN {{ ref('dim_address') }} addr  
            ON cust.`Country` = addr.country
            AND cust.`State` = addr.state
            AND cust.`City` = addr.city
            AND cust.`Postal Code` = addr.postal_code
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'address_key']) }} AS customer_key,
    customer_id,
    customer_name,
    segment,
    address_key 
FROM customer_address

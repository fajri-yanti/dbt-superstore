{{
  config(
    materialized='table'
  )
}}

WITH dim_order AS (
    SELECT DISTINCT
        `Row ID` as row_id,
        `Order ID` as order_id,
        `Order Date` as order_date,
        `Customer ID` as customer_id,
        `Ship Date` as ship_date,
        `Ship Mode` as ship_mode,
        {{ dbt_utils.generate_surrogate_key([
            '`Row ID`', 
            '`Order ID`'
        ]) }} as order_key
    FROM {{source('bronze','transaction_superstore')}}
    WHERE `Order ID` IS NOT NULL
)

SELECT * FROM dim_order
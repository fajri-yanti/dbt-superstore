{{
  config(
    materialized='table'
  )
}}

WITH fact_order AS (
    SELECT 
        do.order_date,
        do.order_key,
        dc.customer_key,
        dp.product_key,
        ts.Sales as sales,
        ts.Quantity as quantity,
        ts.Discount as discount,
        ts.Profit as profit
    FROM {{ source('bronze', 'transaction_superstore') }} ts
    INNER JOIN {{ ref('superstore', 'dim_order') }} do
        ON ts.`Row ID` = do.row_id
        AND ts.`Order ID` = do.order_id
    INNER JOIN {{ ref('superstore', 'dim_address') }} da 
        ON ts.`Postal Code` = da.postal_code
        AND ts.City = da.city
        AND ts.State = da.state
        AND ts.Country = da.country
    INNER JOIN {{ ref('superstore', 'dim_customer') }} dc 
        ON do.customer_id = dc.customer_id 
        AND da.address_key = dc.address_key
    INNER JOIN {{ ref('superstore', 'dim_product') }} dp
        ON ts.`Product ID` = dp.product_id
        AND ts.`Product Name` = dp.product_name
)

SELECT 
    order_date,
    order_key,
    customer_key,
    product_key,
    SUM(sales) AS total_sales,
    SUM(quantity) AS total_quantity,
    SUM(discount) AS total_discount,
    SUM(profit) AS total_profit 
FROM fact_order
GROUP BY order_date, order_key, customer_key, product_key
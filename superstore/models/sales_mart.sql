{{
  config(
    materialized='table',
  )
}}

WITH sales_data AS (
    SELECT 
        do.order_date,
        do.order_id,
        dc.customer_id,
        dc.segment,
        dp.category,
        da.city,
        da.state,
        fs.total_sales,
        fs.total_quantity,
        fs.total_discount,
        fs.total_profit
    FROM {{ ref('fact_sales') }} fs 
    INNER JOIN {{ ref('dim_order') }} do
        ON fs.order_key = do.order_key
    INNER JOIN {{ ref('dim_customer') }} dc
        ON fs.customer_key = dc.customer_key
    INNER JOIN {{ ref('dim_product') }} dp
        ON fs.product_key = dp.product_key
    INNER JOIN {{ ref('dim_address') }} da
        ON fs.address_key = da.address_key
)

SELECT 
    DATE_TRUNC(order_date, YEAR) AS year,
    DATE_TRUNC(order_date, MONTH) AS month, 
    order_id,
    state,
    city,
    category,
    customer_id,
    segment,
    SUM(total_sales) AS total_sales,
    SUM(total_quantity) AS total_quantity,
    SUM(total_discount) AS total_discount,
    SUM(total_profit) AS total_profit
FROM sales_data
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY year, month, state, city, category, customer_id, segment

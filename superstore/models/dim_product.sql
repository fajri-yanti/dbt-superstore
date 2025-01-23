-- dim_product
{{
  config(
    materialized='table'
  )
}}

With product AS (
SELECT DISTINCT 
  `Product ID` AS product_id, 
  `Product Name` AS product_name,
  `Category` AS category,
  `Sub-Category` AS sub_category
FROM
    {{ source('bronze', 'transaction_superstore') }}
)

SELECT 
{{ dbt_utils.generate_surrogate_key([
				'product_id', 
				'product_name'
			])}} AS product_key, *
FROM product

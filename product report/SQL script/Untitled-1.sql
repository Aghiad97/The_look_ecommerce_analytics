/*
===============================================================================
Product Performance Report
===============================================================================
Purpose:
  Provide a detailed, product-level analysis to understand sales trends,
  profitability, customer engagement, and return behavior for products
  in the Look e-commerce dataset.

Key Questions Answered:
  1. Which products are driving revenue?
     – Total sales, gross profit, and customer count by product
     – Average selling and retail prices
  2. How are products performing over time?
     – Lifespan (time span between first and last orders)
     – Monthly sales contribution (avg_monthly_value)
     – Product recency: time since last sale
  3. How efficient are products operationally?
     – Return rate (percent of orders returned)
     – AOV (Average Order Value) per product
     – Performance segmentation (low, mid-range, high)
  4. What are our most and least successful product categories?
     – Sales, order volume, and customer coverage by category

Analyst Notes:
  • Return rate is calculated as percentage of orders returned per product.
  • Performance bands are based on total sales thresholds.
  • You can build on this with visualizations grouped by category, time, etc.
  • Gross profit assumes cost field in product table is COGS per unit.
===============================================================================
*/

WITH base_query AS (
  /*-----------------------------------------------------------------------------
   Step 1: Join orders with product data
   -----------------------------------------------------------------------------*/
  SELECT 
    oi.status,
    oi.created_at,
    oi.returned_at,
    oi.sale_price,
    p.retail_price,
    oi.user_id,
    oi.order_id,
    p.id AS product_id,
    p.name,
    p.cost,
    p.category
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.products` p
    ON oi.product_id = p.id
),

product_level_aggregation AS (
  /*-----------------------------------------------------------------------------
   Step 2: Aggregate key metrics at the product level
   -----------------------------------------------------------------------------*/
  SELECT 
    product_id,
    name,
    category,
    status,
    
    /* Revenue and cost */
    SUM(sale_price) AS total_sales,
    SUM(cost) AS total_cost,

    /* Unique customers and order count */
    COUNT(DISTINCT user_id) AS total_customers,
    COUNT(DISTINCT order_id) AS total_orders,

    /* First and last purchase dates */
    MIN(created_at) AS first_order,
    MAX(created_at) AS last_order,

    /* Duration between first and last order in months */
    DATE_DIFF(CAST(MAX(created_at) AS DATE), CAST(MIN(created_at) AS DATE), MONTH) AS life_span,

    /* Pricing */
    AVG(sale_price) AS avg_selling_price,
    AVG(retail_price) AS avg_retail_price,

    /* Return rate: % of orders that were returned */
    ROUND(SUM(CASE WHEN returned_at IS NOT NULL THEN 1 ELSE 0 END) / COUNT(DISTINCT order_id), 2) AS return_rate

  FROM base_query
  GROUP BY 1, 2, 3, 4
)

SELECT 
  /* Product details */
  product_id,
  name,
  category,
  status,
  life_span,

  /* Sales volume */
  total_sales,

  /* Performance classification based on sales threshold */
  CASE 
    WHEN total_sales < 1000 THEN 'low_performance'
    WHEN total_sales BETWEEN 1000 AND 2500 THEN 'mid-range'
    ELSE 'high_performance'
  END AS product_performance,

  /* Cost and profit */
  ROUND(total_cost, 2) AS total_cost,
  ROUND(total_sales - total_cost, 2) AS gross_profit,

  /* Pricing metrics */
  ROUND(avg_selling_price, 2) AS avg_selling_price,
  ROUND(avg_retail_price, 2) AS avg_retail_price,

  /* Recency: time since last sale in months */
  CASE WHEN last_order IS NULL THEN NULL
       ELSE DATE_DIFF(CAST(CURRENT_DATE() AS DATE), CAST(last_order AS DATE), MONTH) 
  END AS recency,

  /* Return ratio */
  return_rate,

  /* Customer engagement */
  total_customers,
  total_orders,
  first_order,
  last_order,

  /* Efficiency: how much value per order */
  CASE 
    WHEN total_orders = 0 THEN 0
    ELSE ROUND(total_sales / total_orders, 2)
  END AS avg_order_value,

  /* Monthly sales contribution */
  CASE 
    WHEN life_span = 0 THEN total_sales
    ELSE ROUND(total_sales / life_span, 2)
  END AS avg_monthly_value

FROM product_level_aggregation

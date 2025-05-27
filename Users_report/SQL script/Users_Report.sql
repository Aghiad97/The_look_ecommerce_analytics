/*
===============================================================================
Users Report
===============================================================================
Purpose:
  Provide a comprehensive, customer-level view of behavior and value in the Look 
  e-commerce dataset. This report helps answer key business questions around 
  acquisition, retention, returns, and customer segmentation.

Key Questions Answered:
  1. Who are our customers?
     – Total orders and total dollars spent per customer
     – Age, gender, country, city, and traffic source breakdown
  2. How and when do they shop?
     – Date of first and last purchase (purchase cadence)
     – Customer “lifespan” in months between first and last order
     – Average order value (AOV) and average monthly spend
  3. Are they returning or churning?
     – Return rate (percent of orders returned)
     – Churn flag (no orders in the last 6+ months)
  4. How valuable are they?
     – Tiered segments by total spend (low / medium / high)
     – Tenure-based segments (new, early-stage, mid-term, veteran)
  5. Which channels perform best?
     – Average order value by traffic source (marketing channel efficiency)

Analyst Notes:
  • We use a two-step approach:
    1. Base_Query: Join order items with user demographics.
    2. User_Aggregation: Roll up to one row per customer and compute all KPIs.
  • Return rate is calculated at the order level.
  • Churn is defined as no purchases in the past 6 months.
  • Value and tenure thresholds can be tuned based on distribution or business rules.
===============================================================================
*/

WITH Base_Query AS (
  /*-----------------------------------------------------------------------------
   Step 1: Pull raw order + user data
  -----------------------------------------------------------------------------*/
  SELECT 
    oi.order_id,
    oi.product_id,
    ROUND(oi.sale_price, 2)        AS sale_price,
    oi.created_at,
    oi.returned_at,
    oi.status,
    u.id                          AS user_id,
    CONCAT(u.first_name, ' ', u.last_name) AS user_name,
    u.age,
    u.gender,
    u.country,
    u.city,
    u.traffic_source
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.users` u
    ON oi.user_id = u.id
),

User_Aggregation AS (
  /*-----------------------------------------------------------------------------
   Step 2: Aggregate to one row per customer
  -----------------------------------------------------------------------------*/
  SELECT 
    user_id,
    user_name,
    age,
    gender,
    country,
    city,
    status,
    traffic_source,
    /* Months between first & last order */
    DATE_DIFF(CAST(MAX(created_at) AS DATE),
              CAST(MIN(created_at) AS DATE),
              MONTH)                        AS life_span,
    /* First and last purchase timestamps */
    MIN(created_at)                    AS first_order,
    MAX(created_at)                    AS last_order,
    /* Core order & spend metrics */
    COUNT(DISTINCT order_id)           AS total_orders,
    ROUND(SUM(sale_price), 2)          AS total_sales,
    /* Return rate: % of orders with a non-null returned_at */
    ROUND(SUM(CASE WHEN returned_at IS NOT NULL THEN 1 ELSE 0 END)
         / COUNT(order_id), 2)          AS return_rate
  FROM Base_Query
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT 
  /* Basic identifiers */
  user_id,
  user_name,
  country,
  age,
  
  /* Demographic segment */
  CASE
    WHEN age < 20             THEN 'below 20'
    WHEN age BETWEEN 20 AND 35 THEN '20-35'
    WHEN age BETWEEN 35 AND 50 THEN '35-50'
    ELSE 'Above 50'
  END                               AS user_age_group,

  /* Tenure segment based on customer lifespan */
  CASE
    WHEN life_span < 12         THEN 'New'
    WHEN life_span BETWEEN 12 AND 36 THEN 'early-stage'
    WHEN life_span BETWEEN 36 AND 48 THEN 'mid-term'
    ELSE 'Veteran'
  END                               AS user_segments,

  gender,
  total_orders,
  total_sales,

  /* Average order value per customer */
  CASE 
    WHEN total_orders = 0 THEN 0
    ELSE total_sales / total_orders
  END                               AS avg_order_value,

  /* Average spend per month of active life */
  CASE
    WHEN life_span = 0 THEN total_sales
    ELSE total_sales / life_span
  END                               AS avg_monthly_spend,

  traffic_source,

  /* Channel efficiency: average order value by traffic source */
  CASE
    WHEN SUM(total_sales) OVER(PARTITION BY traffic_source) = 0 THEN 0
    ELSE SUM(total_sales) OVER(PARTITION BY traffic_source)
         / SUM(total_orders) OVER(PARTITION BY traffic_source)
  END                               AS traffic_source_avg_order_value,

  /* Purchase timing & returns */
  first_order,
  last_order,
  return_rate,

  /* Churn flag: no orders in the last 6 months */
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), CAST(last_order AS DATE), MONTH) > 5 THEN TRUE
    ELSE FALSE
  END                               AS is_churned,

  /* Customer value tier */
  CASE
    WHEN total_sales < 200          THEN 'low'
    WHEN total_sales BETWEEN 200 AND 600 THEN 'medium'
    ELSE 'high'
  END                               AS customer_value_segments

FROM User_Aggregation;

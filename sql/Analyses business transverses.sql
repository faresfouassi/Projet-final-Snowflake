-- =============================================================================
-- PARTIE 2.3 – ANALYSES BUSINESS TRANSVERSES
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE BERBERE_LAB;
USE SCHEMA SILVER;
USE WAREHOUSE WH_BRB_LAB;
-- -----------------------------------------------------------------------------
-- 2.3.1 - Ventes et Promotions
-- -----------------------------------------------------------------------------
SELECT '=== 2.3.1 - VENTES ET PROMOTIONS ===' AS analysis;

-- Comparaison ventes avec / sans promotion
-- Note : Nécessite une jointure conceptuelle entre transactions et promotions
-- Simulation basée sur les périodes de promotions actives

WITH sales_promo_overlap AS (
  SELECT
    ft.transaction_id,
    ft.transaction_date,
    ft.amount,
    ft.region,
    p.promotion_id,
    p.product_category,
    p.discount_percentage,
    CASE 
      WHEN p.promotion_id IS NOT NULL THEN 'Avec promotion'
      ELSE 'Sans promotion'
    END AS promo_status
  FROM financial_transactions_clean ft
  LEFT JOIN promotions_clean p
    ON ft.region = p.region
    AND ft.transaction_date BETWEEN p.start_date AND p.end_date
  WHERE ft.transaction_type = 'Sale'
    AND ft.amount IS NOT NULL
)
SELECT
  promo_status,
  COUNT(*) AS nb_transactions,
  ROUND(SUM(amount), 2) AS total_sales,
  ROUND(AVG(amount), 2) AS avg_transaction_value,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_volume
FROM sales_promo_overlap
GROUP BY promo_status
ORDER BY total_sales DESC;

-- Sensibilité des catégories aux promotions
SELECT
  product_category,
  COUNT(DISTINCT promotion_id) AS nb_promotions,
  ROUND(AVG(discount_percentage) * 100, 2) AS avg_discount_pct,
  MIN(discount_percentage * 100) AS min_discount,
  MAX(discount_percentage * 100) AS max_discount,
  ROUND(AVG(DATEDIFF(day, start_date, end_date)), 1) AS avg_promo_duration_days
FROM promotions_clean
WHERE product_category IS NOT NULL
  AND discount_percentage IS NOT NULL
GROUP BY product_category
ORDER BY nb_promotions DESC;

-- Impact du niveau de discount
SELECT
  CASE
    WHEN discount_percentage < 0.10 THEN '< 10%'
    WHEN discount_percentage BETWEEN 0.10 AND 0.19 THEN '10-19%'
    WHEN discount_percentage BETWEEN 0.20 AND 0.29 THEN '20-29%'
    WHEN discount_percentage BETWEEN 0.30 AND 0.39 THEN '30-39%'
    ELSE '40%+'
  END AS discount_range,
  COUNT(*) AS nb_promotions,
  COUNT(DISTINCT product_category) AS nb_categories
FROM promotions_clean
WHERE discount_percentage IS NOT NULL
GROUP BY discount_range
ORDER BY discount_range;


-- -----------------------------------------------------------------------------
-- 2.3.2 - Marketing et Performance Commerciale
-- -----------------------------------------------------------------------------
SELECT '=== 2.3.2 - MARKETING ET PERFORMANCE COMMERCIALE ===' AS analysis;

-- Lien campagnes ↔ ventes (par région et période)
WITH campaign_sales AS (
  SELECT
    mc.campaign_id,
    mc.campaign_name,
    mc.campaign_type,
    mc.region,
    mc.budget,
    mc.reach,
    mc.conversion_rate,
    mc.start_date,
    mc.end_date,
    COUNT(ft.transaction_id) AS nb_sales_in_period,
    ROUND(SUM(ft.amount), 2) AS total_sales_in_period
  FROM marketing_campaigns_clean mc
  LEFT JOIN financial_transactions_clean ft
    ON mc.region = ft.region
    AND ft.transaction_date BETWEEN mc.start_date AND mc.end_date
    AND ft.transaction_type = 'Sale'
  WHERE mc.campaign_id IS NOT NULL
  GROUP BY 
    mc.campaign_id, mc.campaign_name, mc.campaign_type, mc.region,
    mc.budget, mc.reach, mc.conversion_rate, mc.start_date, mc.end_date
)
SELECT
  campaign_id,
  campaign_name,
  campaign_type,
  region,
  budget,
  reach,
  ROUND(conversion_rate * 100, 2) AS conversion_rate_pct,
  nb_sales_in_period,
  total_sales_in_period,
  ROUND(total_sales_in_period / NULLIF(budget, 0), 2) AS roi_ratio,
  ROUND(total_sales_in_period / NULLIF(reach, 0), 4) AS revenue_per_reach
FROM campaign_sales
WHERE budget > 0
ORDER BY roi_ratio DESC NULLS LAST
LIMIT 20;

-- Identification des campagnes les plus efficaces
SELECT
  campaign_type,
  COUNT(*) AS nb_campaigns,
  ROUND(SUM(budget), 2) AS total_budget,
  ROUND(AVG(budget), 2) AS avg_budget,
  ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_pct,
  ROUND(AVG(reach), 0) AS avg_reach
FROM marketing_campaigns_clean
WHERE campaign_type IS NOT NULL
  AND budget > 0
GROUP BY campaign_type
ORDER BY avg_conversion_pct DESC;

-- Efficacité par public cible
SELECT
  target_audience,
  COUNT(*) AS nb_campaigns,
  ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_pct,
  ROUND(SUM(budget), 2) AS total_budget,
  ROUND(AVG(reach), 0) AS avg_reach
FROM marketing_campaigns_clean
WHERE target_audience IS NOT NULL
  AND conversion_rate IS NOT NULL
GROUP BY target_audience
ORDER BY avg_conversion_pct DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- 2.3.3 - Expérience Client
-- -----------------------------------------------------------------------------
SELECT '=== 2.3.3 - EXPÉRIENCE CLIENT ===' AS analysis;

-- Impact des avis produits sur les ventes
-- Corrélation entre rating moyen et volume de reviews
WITH product_performance AS (
  SELECT
    product_id,
    product_category,
    COUNT(*) AS nb_reviews,
    ROUND(AVG(rating), 2) AS avg_rating,
    STDDEV(rating) AS rating_stddev
  FROM product_reviews_clean
  WHERE product_id IS NOT NULL
    AND rating IS NOT NULL
  GROUP BY product_id, product_category
)
SELECT
  CASE
    WHEN avg_rating >= 4.5 THEN 'Excellent (4.5-5.0)'
    WHEN avg_rating >= 4.0 THEN 'Très bon (4.0-4.4)'
    WHEN avg_rating >= 3.5 THEN 'Bon (3.5-3.9)'
    WHEN avg_rating >= 3.0 THEN 'Moyen (3.0-3.4)'
    ELSE 'Faible (< 3.0)'
  END AS rating_category,
  COUNT(*) AS nb_products,
  ROUND(AVG(nb_reviews), 1) AS avg_reviews_per_product,
  SUM(nb_reviews) AS total_reviews
FROM product_performance
GROUP BY rating_category
ORDER BY rating_category;


-- Influence des interactions service client
-- Satisfaction client par type d'interaction
SELECT
  interaction_type,
  issue_category,
  COUNT(*) AS nb_interactions,
  ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction,
  ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
  SUM(CASE WHEN resolution_status = 'Resolved' THEN 1 ELSE 0 END) AS nb_resolved,
  ROUND(SUM(CASE WHEN resolution_status = 'Resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS resolution_rate_pct
FROM customer_service_interactions_clean
WHERE interaction_type IS NOT NULL
  AND issue_category IS NOT NULL
GROUP BY interaction_type, issue_category
ORDER BY avg_satisfaction DESC
LIMIT 20;

-- Corrélation entre durée d'interaction et satisfaction
SELECT
  CASE
    WHEN duration_minutes < 5 THEN '< 5 min'
    WHEN duration_minutes BETWEEN 5 AND 9 THEN '5-9 min'
    WHEN duration_minutes BETWEEN 10 AND 19 THEN '10-19 min'
    WHEN duration_minutes BETWEEN 20 AND 29 THEN '20-29 min'
    ELSE '30+ min'
  END AS duration_bucket,
  COUNT(*) AS nb_interactions,
  ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction
FROM customer_service_interactions_clean
WHERE duration_minutes IS NOT NULL
  AND customer_satisfaction IS NOT NULL
GROUP BY duration_bucket
ORDER BY duration_bucket;


-- -----------------------------------------------------------------------------
-- 2.3.4 - Opérations et Logistique
-- -----------------------------------------------------------------------------
SELECT '=== 2.3.4 - OPÉRATIONS ET LOGISTIQUE ===' AS analysis;

-- Ruptures de stock
SELECT
  'Situation actuelle des stocks' AS metric,
  SUM(CASE WHEN current_stock < reorder_point THEN 1 ELSE 0 END) AS nb_stockouts,
  SUM(CASE WHEN current_stock >= reorder_point THEN 1 ELSE 0 END) AS nb_in_stock,
  COUNT(*) AS total_products,
  ROUND(SUM(CASE WHEN current_stock < reorder_point THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS stockout_rate_pct
FROM inventory_clean
WHERE current_stock IS NOT NULL
  AND reorder_point IS NOT NULL;

-- Produits en rupture par catégorie
SELECT
  product_category,
  COUNT(*) AS nb_products_low_stock,
  SUM(current_stock) AS total_current_stock,
  SUM(reorder_point) AS total_reorder_point,
  ROUND(AVG(lead_time), 1) AS avg_lead_time_days
FROM inventory_clean
WHERE current_stock < reorder_point
  AND product_category IS NOT NULL
GROUP BY product_category
ORDER BY nb_products_low_stock DESC;

-- Ruptures par pays/entrepôt
SELECT
  country,
  warehouse,
  COUNT(*) AS total_products,
  SUM(CASE WHEN current_stock < reorder_point THEN 1 ELSE 0 END) AS nb_stockouts,
  ROUND(SUM(CASE WHEN current_stock < reorder_point THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS stockout_rate_pct
FROM inventory_clean
WHERE country IS NOT NULL
  AND warehouse IS NOT NULL
  AND current_stock IS NOT NULL
  AND reorder_point IS NOT NULL
GROUP BY country, warehouse
ORDER BY stockout_rate_pct DESC
LIMIT 20;

-- Impact des délais de livraison
SELECT
  shipping_method,
  COUNT(*) AS nb_shipments,
  ROUND(AVG(shipping_cost), 2) AS avg_cost,
  ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days,
  MIN(DATEDIFF(day, ship_date, estimated_delivery)) AS min_delivery_days,
  MAX(DATEDIFF(day, ship_date, estimated_delivery)) AS max_delivery_days
FROM logistics_and_shipping_clean
WHERE ship_date IS NOT NULL
  AND estimated_delivery IS NOT NULL
  AND shipping_method IS NOT NULL
GROUP BY shipping_method
ORDER BY avg_delivery_days;

-- Performance logistique par région
SELECT
  destination_region,
  destination_country,
  COUNT(*) AS nb_shipments,
  ROUND(AVG(shipping_cost), 2) AS avg_cost,
  ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days,
  SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END) AS nb_delivered,
  ROUND(SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delivery_success_rate_pct
FROM logistics_and_shipping_clean
WHERE destination_region IS NOT NULL
  AND ship_date IS NOT NULL
GROUP BY destination_region, destination_country
ORDER BY nb_shipments DESC
LIMIT 20;

-- Analyse des retards de livraison potentiels
WITH delivery_analysis AS (
  SELECT
    shipment_id,
    order_id,
    ship_date,
    estimated_delivery,
    DATEDIFF(day, ship_date, estimated_delivery) AS expected_delivery_days,
    shipping_method,
    destination_region,
    status
  FROM logistics_and_shipping_clean
  WHERE ship_date IS NOT NULL
    AND estimated_delivery IS NOT NULL
)
SELECT
  shipping_method,
  COUNT(*) AS nb_shipments,
  ROUND(AVG(expected_delivery_days), 1) AS avg_expected_days,
  SUM(CASE WHEN expected_delivery_days > 7 THEN 1 ELSE 0 END) AS nb_long_delivery,
  ROUND(SUM(CASE WHEN expected_delivery_days > 7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS long_delivery_rate_pct
FROM delivery_analysis
GROUP BY shipping_method
ORDER BY long_delivery_rate_pct DESC;

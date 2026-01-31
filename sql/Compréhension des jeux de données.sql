-- =============================================================================
-- PHASE 2 - ANALYSES EXPLORATOIRES ET BUSINESS
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE BERBERE_LAB;
USE SCHEMA SILVER;
USE WAREHOUSE WH_BRB_LAB;

-- =============================================================================
-- PARTIE 2.1 – COMPRÉHENSION DES JEUX DE DONNÉES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1.1 - Vue d'ensemble : Volumes et périodes par table (Analyse sur toutes les tables)
-- -----------------------------------------------------------------------------
SELECT 'OVERVIEW - Volumétrie et dates des tables SILVER' AS section;

WITH table_stats AS (
  SELECT 'customer_demographics_clean' AS table_name,
         COUNT(*) AS total_rows,
         MIN(date_of_birth) AS date_min,
         MAX(date_of_birth) AS date_max,
         COUNT(DISTINCT customer_id) AS distinct_keys
  FROM customer_demographics_clean
  
  UNION ALL
  
  SELECT 'customer_service_interactions_clean',
         COUNT(*),
         MIN(interaction_date),
         MAX(interaction_date),
         COUNT(DISTINCT interaction_id)
  FROM customer_service_interactions_clean
  
  UNION ALL
  
  SELECT 'financial_transactions_clean',
         COUNT(*),
         MIN(transaction_date),
         MAX(transaction_date),
         COUNT(DISTINCT transaction_id)
  FROM financial_transactions_clean
  
  UNION ALL
  
  SELECT 'promotions_clean',
         COUNT(*),
         MIN(start_date),
         MAX(end_date),
         COUNT(DISTINCT promotion_id)
  FROM promotions_clean
  
  UNION ALL
  
  SELECT 'marketing_campaigns_clean',
         COUNT(*),
         MIN(start_date),
         MAX(end_date),
         COUNT(DISTINCT campaign_id)
  FROM marketing_campaigns_clean
  
  UNION ALL
  
  SELECT 'product_reviews_clean',
         COUNT(*),
         MIN(review_date),
         MAX(review_date),
         COUNT(DISTINCT review_id)
  FROM product_reviews_clean
  
  UNION ALL
  
  SELECT 'logistics_and_shipping_clean',
         COUNT(*),
         MIN(ship_date),
         MAX(estimated_delivery),
         COUNT(DISTINCT shipment_id)
  FROM logistics_and_shipping_clean
  
  UNION ALL
  
  SELECT 'supplier_information_clean',
         COUNT(*),
         NULL,
         NULL,
         COUNT(DISTINCT supplier_id)
  FROM supplier_information_clean
  
  UNION ALL
  
  SELECT 'employee_records_clean',
         COUNT(*),
         MIN(hire_date),
         MAX(hire_date),
         COUNT(DISTINCT employee_id)
  FROM employee_records_clean
  
  UNION ALL
  
  SELECT 'inventory_clean',
         COUNT(*),
         MIN(last_restock_date),
         MAX(last_restock_date),
         COUNT(DISTINCT product_id || '-' || country || '-' || warehouse)
  FROM inventory_clean
  
  UNION ALL
  
  SELECT 'store_locations_clean',
         COUNT(*),
         NULL,
         NULL,
         COUNT(DISTINCT store_id)
  FROM store_locations_clean
)
SELECT 
  table_name,
  total_rows,
  distinct_keys,
  ROUND(distinct_keys::FLOAT / NULLIF(total_rows, 0) * 100, 2) AS pct_unique,
  date_min,
  date_max,
  DATEDIFF(day, date_min, date_max) AS days_coverage
FROM table_stats
ORDER BY table_name;


-- -----------------------------------------------------------------------------
-- 2.1.2 - Analyse détaillée par table : Périmètre métier et colonnes clés
-- -----------------------------------------------------------------------------

-- ===== CUSTOMER_DEMOGRAPHICS_CLEAN =====
SELECT '=== CUSTOMER_DEMOGRAPHICS_CLEAN ===' AS analysis;

-- Périmètre métier : Profil démographique des clients
-- Colonnes clés : customer_id (PK), region, country, annual_income

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS name_null,
  SUM(CASE WHEN date_of_birth IS NULL THEN 1 ELSE 0 END) AS dob_null,
  SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) AS gender_null,
  SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS region_null,
  SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS country_null,
  SUM(CASE WHEN annual_income IS NULL THEN 1 ELSE 0 END) AS income_null
FROM customer_demographics_clean;

SELECT
  'Distribution géographique' AS metric,
  region,
  country,
  COUNT(*) AS nb_clients,
  ROUND(AVG(annual_income), 2) AS avg_income,
  MIN(annual_income) AS min_income,
  MAX(annual_income) AS max_income
FROM customer_demographics_clean
WHERE region IS NOT NULL
GROUP BY region, country
ORDER BY nb_clients DESC
LIMIT 20;

SELECT
  'Distribution par genre' AS metric,
  gender,
  COUNT(*) AS nb_clients,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM customer_demographics_clean
WHERE gender IS NOT NULL
GROUP BY gender
ORDER BY nb_clients DESC;


-- ===== CUSTOMER_SERVICE_INTERACTIONS_CLEAN =====
SELECT '=== CUSTOMER_SERVICE_INTERACTIONS_CLEAN ===' AS analysis;

-- Périmètre métier : Historique des interactions avec le service client
-- Colonnes clés : interaction_id (PK), interaction_date, issue_category, resolution_status

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN interaction_type IS NULL THEN 1 ELSE 0 END) AS type_null,
  SUM(CASE WHEN issue_category IS NULL THEN 1 ELSE 0 END) AS category_null,
  SUM(CASE WHEN resolution_status IS NULL THEN 1 ELSE 0 END) AS status_null,
  SUM(CASE WHEN customer_satisfaction IS NULL THEN 1 ELSE 0 END) AS satisfaction_null
FROM customer_service_interactions_clean;

SELECT
  'Distribution par type et statut' AS metric,
  interaction_type,
  resolution_status,
  COUNT(*) AS nb_interactions,
  ROUND(AVG(duration_minutes), 2) AS avg_duration_min,
  ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction
FROM customer_service_interactions_clean
WHERE interaction_type IS NOT NULL
GROUP BY interaction_type, resolution_status
ORDER BY nb_interactions DESC
LIMIT 15;

SELECT
  'Top catégories de problèmes' AS metric,
  issue_category,
  COUNT(*) AS nb_issues,
  ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction
FROM customer_service_interactions_clean
WHERE issue_category IS NOT NULL
GROUP BY issue_category
ORDER BY nb_issues DESC
LIMIT 10;


-- ===== FINANCIAL_TRANSACTIONS_CLEAN =====
SELECT '=== FINANCIAL_TRANSACTIONS_CLEAN ===' AS analysis;

-- Périmètre métier : Transactions financières (ventes, revenus, coûts)
-- Colonnes clés : transaction_id (PK), transaction_date, transaction_type, amount

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN transaction_type IS NULL THEN 1 ELSE 0 END) AS type_null,
  SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_null,
  SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS payment_null,
  SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS region_null
FROM financial_transactions_clean;

SELECT
  'Distribution par type de transaction' AS metric,
  transaction_type,
  COUNT(*) AS nb_transactions,
  ROUND(SUM(amount), 2) AS total_amount,
  ROUND(AVG(amount), 2) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
FROM financial_transactions_clean
WHERE transaction_type IS NOT NULL
GROUP BY transaction_type
ORDER BY total_amount DESC;

SELECT
  'Distribution par méthode de paiement' AS metric,
  payment_method,
  COUNT(*) AS nb_transactions,
  ROUND(SUM(amount), 2) AS total_amount,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_volume
FROM financial_transactions_clean
WHERE payment_method IS NOT NULL
GROUP BY payment_method
ORDER BY total_amount DESC;


-- ===== PROMOTIONS_CLEAN =====
SELECT '=== PROMOTIONS_CLEAN ===' AS analysis;

-- Périmètre métier : Campagnes promotionnelles par catégorie de produit
-- Colonnes clés : promotion_id (PK), product_category, discount_percentage, start_date, end_date

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) AS category_null,
  SUM(CASE WHEN promotion_type IS NULL THEN 1 ELSE 0 END) AS type_null,
  SUM(CASE WHEN discount_percentage IS NULL THEN 1 ELSE 0 END) AS discount_null,
  SUM(CASE WHEN start_date IS NULL THEN 1 ELSE 0 END) AS start_null,
  SUM(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) AS end_null
FROM promotions_clean;

SELECT
  'Distribution par catégorie de produit' AS metric,
  product_category,
  COUNT(*) AS nb_promotions,
  ROUND(AVG(discount_percentage) * 100, 2) AS avg_discount_pct,
  MIN(discount_percentage * 100) AS min_discount_pct,
  MAX(discount_percentage * 100) AS max_discount_pct,
  ROUND(AVG(DATEDIFF(day, start_date, end_date)), 1) AS avg_duration_days
FROM promotions_clean
WHERE product_category IS NOT NULL
  AND start_date IS NOT NULL
  AND end_date IS NOT NULL
GROUP BY product_category
ORDER BY nb_promotions DESC;

SELECT
  'Distribution par type de promotion' AS metric,
  promotion_type,
  COUNT(*) AS nb_promotions,
  ROUND(AVG(discount_percentage) * 100, 2) AS avg_discount_pct
FROM promotions_clean
WHERE promotion_type IS NOT NULL
GROUP BY promotion_type
ORDER BY nb_promotions DESC;


-- ===== MARKETING_CAMPAIGNS_CLEAN =====
SELECT '=== MARKETING_CAMPAIGNS_CLEAN ===' AS analysis;

-- Périmètre métier : Campagnes marketing avec budget et performance
-- Colonnes clés : campaign_id (PK), campaign_type, budget, reach, conversion_rate

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN campaign_name IS NULL THEN 1 ELSE 0 END) AS name_null,
  SUM(CASE WHEN campaign_type IS NULL THEN 1 ELSE 0 END) AS type_null,
  SUM(CASE WHEN budget IS NULL THEN 1 ELSE 0 END) AS budget_null,
  SUM(CASE WHEN reach IS NULL THEN 1 ELSE 0 END) AS reach_null,
  SUM(CASE WHEN conversion_rate IS NULL THEN 1 ELSE 0 END) AS conversion_null
FROM marketing_campaigns_clean;

SELECT
  'Performance par type de campagne' AS metric,
  campaign_type,
  COUNT(*) AS nb_campaigns,
  ROUND(SUM(budget), 2) AS total_budget,
  ROUND(AVG(budget), 2) AS avg_budget,
  ROUND(AVG(reach), 0) AS avg_reach,
  ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_pct
FROM marketing_campaigns_clean
WHERE campaign_type IS NOT NULL
GROUP BY campaign_type
ORDER BY total_budget DESC;

SELECT
  'Performance par région' AS metric,
  region,
  COUNT(*) AS nb_campaigns,
  ROUND(SUM(budget), 2) AS total_budget,
  ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_pct
FROM marketing_campaigns_clean
WHERE region IS NOT NULL
GROUP BY region
ORDER BY total_budget DESC
LIMIT 10;


-- ===== PRODUCT_REVIEWS_CLEAN =====
SELECT '=== PRODUCT_REVIEWS_CLEAN ===' AS analysis;

-- Périmètre métier : Avis clients sur les produits
-- Colonnes clés : review_id (PK), product_id, rating, review_date

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS product_null,
  SUM(CASE WHEN reviewer_id IS NULL THEN 1 ELSE 0 END) AS reviewer_null,
  SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) AS rating_null,
  SUM(CASE WHEN review_text IS NULL THEN 1 ELSE 0 END) AS text_null
FROM product_reviews_clean;

SELECT
  'Distribution des notes' AS metric,
  rating,
  COUNT(*) AS nb_reviews,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM product_reviews_clean
WHERE rating IS NOT NULL
GROUP BY rating
ORDER BY rating;

SELECT
  'Top catégories par volume d avis' AS metric,
  product_category,
  COUNT(*) AS nb_reviews,
  ROUND(AVG(rating), 2) AS avg_rating,
  MIN(rating) AS min_rating,
  MAX(rating) AS max_rating
FROM product_reviews_clean
WHERE product_category IS NOT NULL
GROUP BY product_category
ORDER BY nb_reviews DESC
LIMIT 15;


-- ===== LOGISTICS_AND_SHIPPING_CLEAN =====
SELECT '=== LOGISTICS_AND_SHIPPING_CLEAN ===' AS analysis;

-- Périmètre métier : Expéditions et logistique
-- Colonnes clés : shipment_id (PK), order_id, ship_date, status, shipping_cost

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS order_null,
  SUM(CASE WHEN ship_date IS NULL THEN 1 ELSE 0 END) AS ship_date_null,
  SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS status_null,
  SUM(CASE WHEN shipping_cost IS NULL THEN 1 ELSE 0 END) AS cost_null,
  SUM(CASE WHEN carrier IS NULL THEN 1 ELSE 0 END) AS carrier_null
FROM logistics_and_shipping_clean;

SELECT
  'Distribution par statut' AS metric,
  status,
  COUNT(*) AS nb_shipments,
  ROUND(AVG(shipping_cost), 2) AS avg_cost,
  ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days
FROM logistics_and_shipping_clean
WHERE status IS NOT NULL
  AND ship_date IS NOT NULL
  AND estimated_delivery IS NOT NULL
GROUP BY status
ORDER BY nb_shipments DESC;

SELECT
  'Performance par transporteur' AS metric,
  carrier,
  COUNT(*) AS nb_shipments,
  ROUND(AVG(shipping_cost), 2) AS avg_cost,
  SUM(shipping_cost) AS total_cost
FROM logistics_and_shipping_clean
WHERE carrier IS NOT NULL
GROUP BY carrier
ORDER BY nb_shipments DESC
LIMIT 10;


-- ===== SUPPLIER_INFORMATION_CLEAN =====
SELECT '=== SUPPLIER_INFORMATION_CLEAN ===' AS analysis;

-- Périmètre métier : Informations fournisseurs
-- Colonnes clés : supplier_id (PK), product_category, reliability_score, quality_rating

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN supplier_name IS NULL THEN 1 ELSE 0 END) AS name_null,
  SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) AS category_null,
  SUM(CASE WHEN reliability_score IS NULL THEN 1 ELSE 0 END) AS reliability_null,
  SUM(CASE WHEN quality_rating IS NULL THEN 1 ELSE 0 END) AS quality_null,
  SUM(CASE WHEN lead_time IS NULL THEN 1 ELSE 0 END) AS lead_time_null
FROM supplier_information_clean;

SELECT
  'Distribution par rating qualité' AS metric,
  quality_rating,
  COUNT(*) AS nb_suppliers,
  ROUND(AVG(reliability_score) * 100, 2) AS avg_reliability_pct,
  ROUND(AVG(lead_time), 1) AS avg_lead_time_days
FROM supplier_information_clean
WHERE quality_rating IS NOT NULL
GROUP BY quality_rating
ORDER BY quality_rating;

SELECT
  'Top régions fournisseurs' AS metric,
  region,
  country,
  COUNT(*) AS nb_suppliers,
  ROUND(AVG(reliability_score) * 100, 2) AS avg_reliability_pct
FROM supplier_information_clean
WHERE region IS NOT NULL
GROUP BY region, country
ORDER BY nb_suppliers DESC
LIMIT 15;


-- ===== EMPLOYEE_RECORDS_CLEAN =====
SELECT '=== EMPLOYEE_RECORDS_CLEAN ===' AS analysis;

-- Périmètre métier : Données RH des employés
-- Colonnes clés : employee_id (PK), department, job_title, salary

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS name_null,
  SUM(CASE WHEN department IS NULL THEN 1 ELSE 0 END) AS dept_null,
  SUM(CASE WHEN job_title IS NULL THEN 1 ELSE 0 END) AS title_null,
  SUM(CASE WHEN salary IS NULL THEN 1 ELSE 0 END) AS salary_null,
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS email_null
FROM employee_records_clean;

SELECT
  'Distribution par département' AS metric,
  department,
  COUNT(*) AS nb_employees,
  ROUND(AVG(salary), 2) AS avg_salary,
  MIN(salary) AS min_salary,
  MAX(salary) AS max_salary
FROM employee_records_clean
WHERE department IS NOT NULL
GROUP BY department
ORDER BY nb_employees DESC;

SELECT
  'Distribution géographique employés' AS metric,
  region,
  country,
  COUNT(*) AS nb_employees,
  ROUND(AVG(salary), 2) AS avg_salary
FROM employee_records_clean
WHERE region IS NOT NULL
GROUP BY region, country
ORDER BY nb_employees DESC
LIMIT 10;


-- ===== INVENTORY_CLEAN =====
SELECT '=== INVENTORY_CLEAN ===' AS analysis;

-- Périmètre métier : Stocks par produit et entrepôt
-- Colonnes clés : (product_id, country, warehouse), current_stock, reorder_point

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS product_null,
  SUM(CASE WHEN warehouse IS NULL THEN 1 ELSE 0 END) AS warehouse_null,
  SUM(CASE WHEN current_stock IS NULL THEN 1 ELSE 0 END) AS stock_null,
  SUM(CASE WHEN reorder_point IS NULL THEN 1 ELSE 0 END) AS reorder_null,
  SUM(CASE WHEN last_restock_date IS NULL THEN 1 ELSE 0 END) AS date_null
FROM inventory_clean;

SELECT
  'Situation de stock' AS metric,
  CASE 
    WHEN current_stock < reorder_point THEN 'Rupture / Besoin réappro'
    WHEN current_stock BETWEEN reorder_point AND reorder_point * 2 THEN 'Stock normal'
    ELSE 'Surstock'
  END AS stock_status,
  COUNT(*) AS nb_products,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inventory_clean
WHERE current_stock IS NOT NULL
  AND reorder_point IS NOT NULL
GROUP BY stock_status
ORDER BY nb_products DESC;

SELECT
  'Stock par catégorie' AS metric,
  product_category,
  COUNT(DISTINCT product_id) AS nb_products,
  SUM(current_stock) AS total_stock,
  ROUND(AVG(current_stock), 1) AS avg_stock
FROM inventory_clean
WHERE product_category IS NOT NULL
GROUP BY product_category
ORDER BY total_stock DESC
LIMIT 15;


-- ===== STORE_LOCATIONS_CLEAN =====
SELECT '=== STORE_LOCATIONS_CLEAN ===' AS analysis;

-- Périmètre métier : Localisation et caractéristiques des magasins
-- Colonnes clés : store_id, store_type, region, square_footage, employee_count

SELECT
  'Valeurs manquantes' AS metric_type,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN store_name IS NULL THEN 1 ELSE 0 END) AS name_null,
  SUM(CASE WHEN store_type IS NULL THEN 1 ELSE 0 END) AS type_null,
  SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS region_null,
  SUM(CASE WHEN square_footage IS NULL THEN 1 ELSE 0 END) AS sqft_null,
  SUM(CASE WHEN employee_count IS NULL THEN 1 ELSE 0 END) AS emp_count_null
FROM store_locations_clean;

SELECT
  'Distribution par type de magasin' AS metric,
  store_type,
  COUNT(*) AS nb_stores,
  ROUND(AVG(square_footage), 0) AS avg_sqft,
  ROUND(AVG(employee_count), 1) AS avg_employees
FROM store_locations_clean
WHERE store_type IS NOT NULL
GROUP BY store_type
ORDER BY nb_stores DESC;

SELECT
  'Répartition géographique magasins' AS metric,
  region,
  country,
  COUNT(*) AS nb_stores,
  SUM(employee_count) AS total_employees
FROM store_locations_clean
WHERE region IS NOT NULL
GROUP BY region, country
ORDER BY nb_stores DESC
LIMIT 15;


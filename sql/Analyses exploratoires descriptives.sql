-- =============================================================================
-- PARTIE 2.2 – ANALYSES EXPLORATOIRES DESCRIPTIVES
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE BERBERE_LAB;
USE SCHEMA SILVER;
USE WAREHOUSE WH_BRB_LAB;
-- -----------------------------------------------------------------------------
-- 2.2.1 - Évolution des ventes dans le temps
-- -----------------------------------------------------------------------------
SELECT '=== 2.2.1 - ÉVOLUTION DES VENTES DANS LE TEMPS ===' AS analysis;

-- Ventes mensuelles
WITH monthly_sales AS (
  SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_sales,
    ROUND(AVG(amount), 2) AS avg_transaction_value
  FROM financial_transactions_clean
  WHERE transaction_type = 'Sale'
    AND transaction_date IS NOT NULL
    AND amount IS NOT NULL
  GROUP BY month
)
SELECT
  month,
  nb_transactions,
  total_sales,
  avg_transaction_value,
  ROUND((total_sales - LAG(total_sales) OVER (ORDER BY month)) / NULLIF(LAG(total_sales) OVER (ORDER BY month), 0) * 100, 2) AS growth_pct_mom
FROM monthly_sales
ORDER BY month;

-- Ventes par trimestre
SELECT
  DATE_TRUNC('quarter', transaction_date) AS quarter,
  COUNT(*) AS nb_transactions,
  ROUND(SUM(amount), 2) AS total_sales,
  ROUND(AVG(amount), 2) AS avg_transaction_value
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
  AND transaction_date IS NOT NULL
  AND amount IS NOT NULL
GROUP BY quarter
ORDER BY quarter;

-- Tendance des ventes par jour de la semaine
SELECT
  DAYNAME(transaction_date) AS day_of_week,
  DAYOFWEEK(transaction_date) AS day_num,
  COUNT(*) AS nb_transactions,
  ROUND(SUM(amount), 2) AS total_sales,
  ROUND(AVG(amount), 2) AS avg_transaction_value
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
  AND transaction_date IS NOT NULL
  AND amount IS NOT NULL
GROUP BY day_of_week, day_num
ORDER BY day_num;


-- -----------------------------------------------------------------------------
-- 2.2.2 - Performance par produit, catégorie et région
-- -----------------------------------------------------------------------------
SELECT '=== 2.2.2 - PERFORMANCE PAR CATÉGORIE ET RÉGION ===' AS analysis;

-- Note : Les données de ventes ne contiennent pas directement les catégories de produits
-- On utilise les proxies disponibles via les promotions et les reviews

-- Performance par région (basée sur financial_transactions)
SELECT
  region,
  COUNT(*) AS nb_transactions,
  ROUND(SUM(amount), 2) AS total_sales,
  ROUND(AVG(amount), 2) AS avg_transaction,
  ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_total_sales
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
  AND region IS NOT NULL
  AND amount IS NOT NULL
GROUP BY region
ORDER BY total_sales DESC;

-- Top catégories par nombre d'avis (proxy de popularité)
SELECT
  product_category,
  COUNT(*) AS nb_reviews,
  ROUND(AVG(rating), 2) AS avg_rating,
  COUNT(DISTINCT product_id) AS nb_products
FROM product_reviews_clean
WHERE product_category IS NOT NULL
GROUP BY product_category
ORDER BY nb_reviews DESC
LIMIT 20;

-- Top produits par volume d'avis
SELECT
  product_id,
  product_category,
  COUNT(*) AS nb_reviews,
  ROUND(AVG(rating), 2) AS avg_rating,
  MIN(rating) AS min_rating,
  MAX(rating) AS max_rating
FROM product_reviews_clean
WHERE product_id IS NOT NULL
GROUP BY product_id, product_category
ORDER BY nb_reviews DESC
LIMIT 30;


-- -----------------------------------------------------------------------------
-- 2.2.3 - Répartition des clients par segments démographiques
-- -----------------------------------------------------------------------------
SELECT '=== 2.2.3 - SEGMENTATION CLIENTS DÉMOGRAPHIQUE ===' AS analysis;

-- Distribution par tranche d'âge
WITH customer_age AS (
  SELECT
    customer_id,
    name,
    gender,
    region,
    annual_income,
    YEAR(CURRENT_DATE) - YEAR(date_of_birth) AS age
  FROM customer_demographics_clean
  WHERE date_of_birth IS NOT NULL
)
SELECT
  CASE
    WHEN age < 25 THEN '18-24'
    WHEN age BETWEEN 25 AND 34 THEN '25-34'
    WHEN age BETWEEN 35 AND 44 THEN '35-44'
    WHEN age BETWEEN 45 AND 54 THEN '45-54'
    WHEN age BETWEEN 55 AND 64 THEN '55-64'
    ELSE '65+'
  END AS age_group,
  COUNT(*) AS nb_customers,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_customers,
  ROUND(AVG(annual_income), 2) AS avg_income
FROM customer_age
GROUP BY age_group
ORDER BY age_group;

-- Segmentation par revenu
SELECT
  CASE
    WHEN annual_income < 30000 THEN '< 30K'
    WHEN annual_income BETWEEN 30000 AND 49999 THEN '30K-50K'
    WHEN annual_income BETWEEN 50000 AND 74999 THEN '50K-75K'
    WHEN annual_income BETWEEN 75000 AND 99999 THEN '75K-100K'
    ELSE '100K+'
  END AS income_bracket,
  COUNT(*) AS nb_customers,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_customers
FROM customer_demographics_clean
WHERE annual_income IS NOT NULL
GROUP BY income_bracket
ORDER BY 
  CASE income_bracket
    WHEN '< 30K' THEN 1
    WHEN '30K-50K' THEN 2
    WHEN '50K-75K' THEN 3
    WHEN '75K-100K' THEN 4
    ELSE 5
  END;

-- Segmentation par région et genre
SELECT
  region,
  gender,
  COUNT(*) AS nb_customers,
  ROUND(AVG(annual_income), 2) AS avg_income,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY region), 2) AS pct_in_region
FROM customer_demographics_clean
WHERE region IS NOT NULL
  AND gender IS NOT NULL
GROUP BY region, gender
ORDER BY region, nb_customers DESC;


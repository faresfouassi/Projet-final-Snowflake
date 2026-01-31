-- =============================================================================
-- ÉTAPE 5 – DATA CLEANING
-- 
-- Principes appliqués :
--  • Gestion des valeurs manquantes (NULLIF, COALESCE, valeurs par défaut au besoin)
--  • Suppression/traitement des doublons (ROW_NUMBER() OVER (...) WHERE rn = 1)
--  • Harmonisation des formats de dates (TRY_TO_DATE / TO_DATE)
--  • Règles de qualité (montants/coûts >= 0, notes dans un intervalle valide, etc.)
--  • Matérialisation par CTAS (CREATE TABLE AS SELECT) dans le schéma SILVER
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE BERBERE_LAB;
USE WAREHOUSE WH_BRB_LAB;
USE SCHEMA BRONZE;

-- Recréation du schéma SILVER s'il n'existe pas
CREATE SCHEMA IF NOT EXISTS SILVER COMMENT = 'Données nettoyées prêtes pour analyse';

/* ---------------------------------------------------------------------------
   1) CUSTOMER_DEMOGRAPHICS  ->  SILVER.customer_demographics_clean
   - Dates : date_of_birth (DATE)
   - Nettoyages : TRIM/NULLIF ; annual_income >= 0
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.customer_demographics_clean AS
WITH base AS (
  SELECT
    customer_id,
    TRIM(NULLIF(name, ''))                               AS name,
    date_of_birth,
    TRIM(NULLIF(gender, ''))                             AS gender,
    TRIM(NULLIF(region, ''))                             AS region,
    TRIM(NULLIF(country, ''))                            AS country,
    TRIM(NULLIF(city, ''))                               AS city,
    TRIM(NULLIF(marital_status, ''))                     AS marital_status,
    IFF(annual_income < 0, NULL, annual_income)          AS annual_income
  FROM BRONZE.customer_demographics
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn
  FROM base b
)
SELECT 
  customer_id, name, date_of_birth, gender, region, country, 
  city, marital_status, annual_income
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   2) CUSTOMER_SERVICE_INTERACTIONS  ->  SILVER.customer_service_interactions_clean
   - Dates : interaction_date (DATE)
   - Nettoyages : TRIM/NULLIF ; customer_satisfaction ∈ [1,5]
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.customer_service_interactions_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(interaction_id, ''))                     AS interaction_id,
    interaction_date,
    TRIM(NULLIF(interaction_type, ''))                   AS interaction_type,
    TRIM(NULLIF(issue_category, ''))                     AS issue_category,
    TRIM(description)                                    AS description,
    IFF(duration_minutes < 0, NULL, duration_minutes)    AS duration_minutes,
    TRIM(NULLIF(resolution_status, ''))                  AS resolution_status,
    TRIM(NULLIF(follow_up_required, ''))                 AS follow_up_required,
    IFF(customer_satisfaction BETWEEN 1 AND 5, customer_satisfaction, NULL) AS customer_satisfaction
  FROM BRONZE.customer_service_interactions
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY interaction_id ORDER BY interaction_id) AS rn
  FROM base b
)
SELECT 
  interaction_id, interaction_date, interaction_type, issue_category,
  description, duration_minutes, resolution_status, follow_up_required,
  customer_satisfaction
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   3) FINANCIAL_TRANSACTIONS  ->  SILVER.financial_transactions_clean
   - Dates : transaction_date (DATE)
   - Règles : amount >= 0 ; TRIM
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.financial_transactions_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(transaction_id, ''))                     AS transaction_id,
    transaction_date,
    TRIM(NULLIF(transaction_type, ''))                   AS transaction_type,
    IFF(amount < 0, NULL, amount)                        AS amount,
    TRIM(NULLIF(payment_method, ''))                     AS payment_method,
    TRIM(NULLIF(entity, ''))                             AS entity,
    TRIM(NULLIF(region, ''))                             AS region,
    TRIM(NULLIF(account_code, ''))                       AS account_code
  FROM BRONZE.financial_transactions
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
  FROM base b
)
SELECT 
  transaction_id, transaction_date, transaction_type, amount,
  payment_method, entity, region, account_code
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   4) PROMOTIONS_DATA  ->  SILVER.promotions_clean
   - Dates : start_date, end_date (DATE) ; cohérence start<=end
   - Règles : discount_percentage ∈ [0,1]
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.promotions_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(promotion_id, ''))                       AS promotion_id,
    TRIM(NULLIF(product_category, ''))                   AS product_category,
    TRIM(NULLIF(promotion_type, ''))                     AS promotion_type,
    IFF(discount_percentage BETWEEN 0 AND 1, discount_percentage, NULL) AS discount_percentage,
    start_date,
    end_date,
    TRIM(NULLIF(region, ''))                             AS region
  FROM BRONZE.promotions_data
),
fix_dates AS (
  SELECT 
    promotion_id, product_category, promotion_type, discount_percentage,
    start_date, region,
    IFF(start_date IS NOT NULL AND end_date IS NOT NULL AND start_date > end_date,
        NULL, end_date) AS end_date
  FROM base
),
Dedup AS (
  SELECT f.*,
         ROW_NUMBER() OVER (PARTITION BY f.promotion_id ORDER BY f.promotion_id) AS rn
  FROM fix_dates f
)
SELECT 
  promotion_id, product_category, promotion_type, discount_percentage,
  start_date, end_date, region
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   5) MARKETING_CAMPAIGNS  ->  SILVER.marketing_campaigns_clean
   - Dates : start_date, end_date (DATE)
   - Règles : budget >= 0 ; conversion_rate ∈ [0,1] ; reach >= 0
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.marketing_campaigns_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(campaign_id, ''))                        AS campaign_id,
    TRIM(NULLIF(campaign_name, ''))                      AS campaign_name,
    TRIM(NULLIF(campaign_type, ''))                      AS campaign_type,
    TRIM(NULLIF(product_category, ''))                   AS product_category,
    TRIM(NULLIF(target_audience, ''))                    AS target_audience,
    start_date,
    end_date,
    TRIM(NULLIF(region, ''))                             AS region,
    IFF(budget < 0, NULL, budget)                        AS budget,
    IFF(reach < 0, NULL, reach)                          AS reach,
    IFF(conversion_rate BETWEEN 0 AND 1, conversion_rate, NULL) AS conversion_rate
  FROM BRONZE.marketing_campaigns
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY campaign_id) AS rn
  FROM base b
)
SELECT 
  campaign_id, campaign_name, campaign_type, product_category, target_audience,
  start_date, end_date, region, budget, reach, conversion_rate
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   6) PRODUCT_REVIEWS  ->  SILVER.product_reviews_clean
   - Dates : review_date (DATE)
   - Règles : rating ∈ [1,5]
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.product_reviews_clean AS
WITH base AS (
  SELECT
    review_id,
    TRIM(NULLIF(product_id, ''))                         AS product_id,
    TRIM(NULLIF(reviewer_id, ''))                        AS reviewer_id,
    TRIM(NULLIF(reviewer_name, ''))                      AS reviewer_name,
    IFF(rating BETWEEN 1 AND 5, rating, NULL)            AS rating,
    review_date,
    TRIM(review_title)                                   AS review_title,
    TRIM(review_text)                                    AS review_text,
    TRIM(NULLIF(product_category, ''))                   AS product_category
  FROM BRONZE.product_reviews
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) AS rn
  FROM base b
)
SELECT 
  review_id, product_id, reviewer_id, reviewer_name, rating,
  review_date, review_title, review_text, product_category
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   7) LOGISTICS_AND_SHIPPING  ->  SILVER.logistics_and_shipping_clean
   - Dates : ship_date, estimated_delivery (DATE)
   - Règles : shipping_cost >= 0 ; cohérence estimated_delivery >= ship_date
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.logistics_and_shipping_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(shipment_id, ''))                         AS shipment_id,
    TRIM(NULLIF(order_id, ''))                            AS order_id,
    ship_date,
    estimated_delivery,
    TRIM(NULLIF(shipping_method, ''))                     AS shipping_method,
    TRIM(NULLIF(status, ''))                              AS status,
    IFF(shipping_cost < 0, NULL, shipping_cost)           AS shipping_cost,
    TRIM(NULLIF(destination_region, ''))                  AS destination_region,
    TRIM(NULLIF(destination_country, ''))                 AS destination_country,
    TRIM(NULLIF(carrier, ''))                             AS carrier
  FROM BRONZE.logistics_and_shipping
),
fix_dates AS (
  SELECT 
    shipment_id, order_id, ship_date, shipping_method, status,
    shipping_cost, destination_region, destination_country, carrier,
    IFF(ship_date IS NOT NULL AND estimated_delivery IS NOT NULL AND estimated_delivery < ship_date,
        NULL, estimated_delivery) AS estimated_delivery
  FROM base
),
Dedup AS (
  SELECT f.*,
         ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY shipment_id) AS rn
  FROM fix_dates f
)
SELECT 
  shipment_id, order_id, ship_date, estimated_delivery,
  shipping_method, status, shipping_cost,
  destination_region, destination_country, carrier
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   8) SUPPLIER_INFORMATION  ->  SILVER.supplier_information_clean
   - Règles : lead_time >= 0 ; reliability_score ∈ [0,1]
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.supplier_information_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(supplier_id, ''))                        AS supplier_id,
    TRIM(NULLIF(supplier_name, ''))                      AS supplier_name,
    TRIM(NULLIF(product_category, ''))                   AS product_category,
    TRIM(NULLIF(region, ''))                             AS region,
    TRIM(NULLIF(country, ''))                            AS country,
    TRIM(NULLIF(city, ''))                               AS city,
    IFF(lead_time < 0, NULL, lead_time)                  AS lead_time,
    IFF(reliability_score BETWEEN 0 AND 1, reliability_score, NULL) AS reliability_score,
    TRIM(NULLIF(quality_rating, ''))                     AS quality_rating
  FROM BRONZE.supplier_information
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY supplier_id) AS rn
  FROM base b
)
SELECT 
  supplier_id, supplier_name, product_category, region, country,
  city, lead_time, reliability_score, quality_rating
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   9) EMPLOYEE_RECORDS  ->  SILVER.employee_records_clean
   - Dates : date_of_birth, hire_date (DATE)
   - Règles : salary >= 0 ; email normalisé (lowercase)
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.employee_records_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(employee_id, ''))                        AS employee_id,
    TRIM(NULLIF(name, ''))                               AS name,
    date_of_birth,
    hire_date,
    TRIM(NULLIF(department, ''))                         AS department,
    TRIM(NULLIF(job_title, ''))                          AS job_title,
    IFF(salary < 0, NULL, salary)                        AS salary,
    TRIM(NULLIF(region, ''))                             AS region,
    TRIM(NULLIF(country, ''))                            AS country,
    LOWER(TRIM(NULLIF(email, '')))                       AS email
  FROM BRONZE.employee_records
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY employee_id) AS rn
  FROM base b
)
SELECT 
  employee_id, name, date_of_birth, hire_date, department,
  job_title, salary, region, country, email
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   10) INVENTORY  ->  SILVER.inventory_clean
   - Dates : last_restock_date (DATE)
   - Règles : current_stock/reorder_point/lead_time >= 0
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.inventory_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(product_id, ''))                         AS product_id,
    TRIM(NULLIF(product_category, ''))                   AS product_category,
    TRIM(NULLIF(region, ''))                             AS region,
    TRIM(NULLIF(country, ''))                            AS country,
    TRIM(NULLIF(warehouse, ''))                          AS warehouse,
    IFF(current_stock < 0, NULL, current_stock)          AS current_stock,
    IFF(reorder_point < 0, NULL, reorder_point)          AS reorder_point,
    IFF(lead_time < 0, NULL, lead_time)                  AS lead_time,
    last_restock_date
  FROM BRONZE.inventory
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (
           PARTITION BY product_id, country, warehouse
           ORDER BY last_restock_date DESC NULLS LAST
         ) AS rn
  FROM base b
)
SELECT 
  product_id, product_category, region, country, warehouse,
  current_stock, reorder_point, lead_time, last_restock_date
FROM Dedup 
WHERE rn = 1;

/* ---------------------------------------------------------------------------
   11) STORE_LOCATIONS  ->  SILVER.store_locations_clean
   - Règles : square_footage >= 0 ; employee_count >= 0
--------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE SILVER.store_locations_clean AS
WITH base AS (
  SELECT
    TRIM(NULLIF(store_id, ''))                           AS store_id,
    TRIM(NULLIF(store_name, ''))                         AS store_name,
    TRIM(NULLIF(store_type, ''))                         AS store_type,
    TRIM(NULLIF(region, ''))                             AS region,
    TRIM(NULLIF(country, ''))                            AS country,
    TRIM(NULLIF(city, ''))                               AS city,
    TRIM(NULLIF(address, ''))                            AS address,
    TRIM(NULLIF(postal_code, ''))                        AS postal_code,
    IFF(square_footage < 0, NULL, square_footage)        AS square_footage,
    IFF(employee_count < 0, NULL, employee_count)        AS employee_count
  FROM BRONZE.store_locations
),
Dedup AS (
  SELECT b.*,
         ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base b
)
SELECT 
  store_id, store_name, store_type, region, country,
  city, address, postal_code, square_footage, employee_count
FROM Dedup 
WHERE rn = 1;

-- =============================================================================
-- CONTRÔLES RAPIDES (post-création) – volumétrie SILVER
-- =============================================================================
SELECT 'customer_demographics_clean'           AS table_name, COUNT(*) AS rows_cnt FROM SILVER.customer_demographics_clean UNION ALL
SELECT 'customer_service_interactions_clean'   AS table_name, COUNT(*) AS rows_cnt FROM SILVER.customer_service_interactions_clean UNION ALL
SELECT 'financial_transactions_clean'          AS table_name, COUNT(*) AS rows_cnt FROM SILVER.financial_transactions_clean UNION ALL
SELECT 'promotions_clean'                      AS table_name, COUNT(*) AS rows_cnt FROM SILVER.promotions_clean UNION ALL
SELECT 'marketing_campaigns_clean'             AS table_name, COUNT(*) AS rows_cnt FROM SILVER.marketing_campaigns_clean UNION ALL
SELECT 'product_reviews_clean'                 AS table_name, COUNT(*) AS rows_cnt FROM SILVER.product_reviews_clean UNION ALL
SELECT 'logistics_and_shipping_clean'          AS table_name, COUNT(*) AS rows_cnt FROM SILVER.logistics_and_shipping_clean UNION ALL
SELECT 'supplier_information_clean'            AS table_name, COUNT(*) AS rows_cnt FROM SILVER.supplier_information_clean UNION ALL
SELECT 'employee_records_clean'                AS table_name, COUNT(*) AS rows_cnt FROM SILVER.employee_records_clean UNION ALL
SELECT 'inventory_clean'                       AS table_name, COUNT(*) AS rows_cnt FROM SILVER.inventory_clean UNION ALL
SELECT 'store_locations_clean'                 AS table_name, COUNT(*) AS rows_cnt FROM SILVER.store_locations_clean
ORDER BY table_name;
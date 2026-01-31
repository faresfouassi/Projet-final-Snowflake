-- =============================================================================
-- Étape 1 – Préparation de l’environnement Snowflake
-- =============================================================================
SET db_name      = 'BERBERE_LAB';
SET wh_name      = 'WH_BRB_LAB';
SET schema_raw   = 'BRONZE';        -- Schéma pour les données brutes
SET schema_ana   = 'SILVER';  -- Schéma pour les tdonnées nétoyées

USE ROLE SYSADMIN;
--Création du Wirehouse
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($wh_name)
  WITH
    WAREHOUSE_SIZE       = 'XSMALL'
    AUTO_SUSPEND         = 60
    AUTO_RESUME          = TRUE
    INITIALLY_SUSPENDED  = TRUE
    COMMENT = 'Warehouse du lab BERBERE';

USE WAREHOUSE IDENTIFIER($wh_name);

-- Création de la base de données Database  
CREATE DATABASE IF NOT EXISTS IDENTIFIER($db_name)
  COMMENT = 'Database du lab BERBERE Food & Beverage';

USE DATABASE IDENTIFIER($db_name);

-- Création des deux schémas BRONZE et SILVER 
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_raw)
  COMMENT = 'Données brutes, au plus proche des fichiers sources';

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_ana)
  COMMENT = 'Tables analytiques prêtes à l’emploi (BI/insights)';

--Vérification de la création
SHOW DATABASES LIKE 'BERBERE_LAB';
SHOW SCHEMAS IN DATABASE IDENTIFIER($db_name);
SHOW WAREHOUSES LIKE 'WH_BRB_LAB';

SELECT
  'Environnement prêt'                                                   AS STATUS,
  'DB: ' || $db_name                                                      AS DATABASE_INFO,
  'Schémas: ' || $schema_raw || ', ' || $schema_ana                        AS SCHEMAS_INFO,
  'WH: ' || $wh_name || ' (XSMALL, auto-suspend 60s, auto-resume)'         AS WAREHOUSE_INFO;

-- Création du STAGE 

USE SCHEMA IDENTIFIER($schema_raw);   

-- Stage pointant vers le bucket public du lab Food & Beverage
CREATE OR REPLACE STAGE STG_FOOD_BEVERAGE
  URL = 's3://logbrain-datalake/datasets/food-beverage/'
  COMMENT = 'Stage S3 public - utilisé pour charger les fichiers du projet Food & Beverage';

-- Vérification de contenu du stage 
LIST @STG_FOOD_BEVERAGE;

-- ==============================================================================================================================================================
-- Étape 2 – Création des tables
-- ==============================================================================================================================================================

USE ROLE SYSADMIN;
USE DATABASE BERBERE_LAB;
USE SCHEMA BRONZE;
USE WAREHOUSE WH_BRB_LAB;

-- 1) CUSTOMER_DEMOGRAPHICS (CSV)
CREATE OR REPLACE TABLE customer_demographics (
    customer_id      NUMBER                 PRIMARY KEY,
    name             VARCHAR(200),
    date_of_birth    DATE,
    gender           VARCHAR(20),
    region           VARCHAR(100),
    country          VARCHAR(100),
    city             VARCHAR(100),
    marital_status   VARCHAR(50),
    annual_income    NUMBER(18,2)
)
COMMENT = 'BRONZE | customer_demographics.csv';

-- 2) CUSTOMER_SERVICE_INTERACTIONS (CSV)
CREATE OR REPLACE TABLE customer_service_interactions (
    interaction_id        VARCHAR(50) PRIMARY KEY,
    interaction_date      DATE,
    interaction_type      VARCHAR(50),
    issue_category        VARCHAR(100),
    description           TEXT,
    duration_minutes      NUMBER(18,2),
    resolution_status     VARCHAR(50),
    follow_up_required    VARCHAR(10),
    customer_satisfaction NUMBER(2,0)
)
COMMENT = 'BRONZE | customer_service_interactions.csv';

-- 3) FINANCIAL_TRANSACTIONS (CSV)
CREATE OR REPLACE TABLE financial_transactions (
    transaction_id   VARCHAR(50) PRIMARY KEY,
    transaction_date DATE,
    transaction_type VARCHAR(50),
    amount           NUMBER(18,2),
    payment_method   VARCHAR(50),
    entity           VARCHAR(200),
    region           VARCHAR(100),
    account_code     VARCHAR(50)
)
COMMENT = 'BRONZE | financial_transactions.csv';

-- 4) PROMOTIONS_DATA (CSV)
CREATE OR REPLACE TABLE promotions_data (
    promotion_id        VARCHAR(50) PRIMARY KEY,
    product_category    VARCHAR(100),
    promotion_type      VARCHAR(100),
    discount_percentage NUMBER(6,4),
    start_date          DATE,
    end_date            DATE,
    region              VARCHAR(100)
)
COMMENT = 'BRONZE | promotions-data.csv';

-- 5) MARKETING_CAMPAIGNS (CSV)
CREATE OR REPLACE TABLE marketing_campaigns (
    campaign_id      VARCHAR(50) PRIMARY KEY,
    campaign_name    VARCHAR(200),
    campaign_type    VARCHAR(100),
    product_category VARCHAR(100),
    target_audience  VARCHAR(100),
    start_date       DATE,
    end_date         DATE,
    region           VARCHAR(100),
    budget           NUMBER(18,2),
    reach            NUMBER(18,0),
    conversion_rate  NUMBER(6,4)
)
COMMENT = 'BRONZE | marketing_campaigns.csv';

-- 6) PRODUCT_REVIEWS (CSV)
CREATE OR REPLACE TABLE product_reviews (
    review_id         NUMBER PRIMARY KEY,
    product_id        VARCHAR(50),
    reviewer_id       VARCHAR(50),
    reviewer_name     VARCHAR(200),
    rating            NUMBER(2,0),
    review_date       DATE,
    review_title      VARCHAR(500),
    review_text       TEXT,
    product_category  VARCHAR(100)
)
COMMENT = 'BRONZE | product_reviews.csv';

-- 7) LOGISTICS_AND_SHIPPING (CSV)
CREATE OR REPLACE TABLE logistics_and_shipping (
    shipment_id          VARCHAR(50) PRIMARY KEY,
    order_id             VARCHAR(50),
    ship_date            DATE,
    estimated_delivery   DATE,
    shipping_method      VARCHAR(50),
    status               VARCHAR(50),
    shipping_cost        NUMBER(18,2),
    destination_region   VARCHAR(100),
    destination_country  VARCHAR(100),
    carrier              VARCHAR(200)
)
COMMENT = 'BRONZE | logistics_and_shipping.csv';

-- 8) SUPPLIER_INFORMATION (CSV)
CREATE OR REPLACE TABLE supplier_information (
    supplier_id        VARCHAR(50) PRIMARY KEY,
    supplier_name      VARCHAR(200),
    product_category   VARCHAR(100),
    region             VARCHAR(100),
    country            VARCHAR(100),
    city               VARCHAR(100),
    lead_time          NUMBER(18,0),
    reliability_score  NUMBER(6,4),     -- 0.86 -> précision suffisante
    quality_rating     VARCHAR(5)       -- ex. A / B / C
)
COMMENT = 'BRONZE | supplier_information.csv';

-- 9) EMPLOYEE_RECORDS (CSV)
CREATE OR REPLACE TABLE employee_records (
    employee_id   VARCHAR(50) PRIMARY KEY,
    name          VARCHAR(200),
    date_of_birth DATE,
    hire_date     DATE,
    department    VARCHAR(100),
    job_title     VARCHAR(200),
    salary        NUMBER(18,2),
    region        VARCHAR(100),
    country       VARCHAR(100),
    email         VARCHAR(254)  -- longueur standard email raisonnable
)
COMMENT = 'BRONZE | employee_records.csv';

-- 10) INVENTORY (JSON -> colonisé)
CREATE OR REPLACE TABLE inventory (
    product_id        VARCHAR(50),
    product_category  VARCHAR(100),
    region            VARCHAR(100),
    country           VARCHAR(100),
    warehouse         VARCHAR(200),
    current_stock     NUMBER(18,0),
    reorder_point     NUMBER(18,0),
    lead_time         NUMBER(18,0),
    last_restock_date DATE
)
COMMENT = 'BRONZE | inventory.json (colonisé)';

-- 11) STORE_LOCATIONS (JSON -> colonisé)
CREATE OR REPLACE TABLE store_locations (
    store_id        VARCHAR(50),
    store_name      VARCHAR(200),
    store_type      VARCHAR(100),
    region          VARCHAR(100),
    country         VARCHAR(100),
    city            VARCHAR(100),
    address         VARCHAR(500),
    postal_code     VARCHAR(20),   -- on garde string (codes postaux non numériques)
    square_footage  NUMBER(18,2),
    employee_count  NUMBER(18,0)
)
COMMENT = 'BRONZE | store_locations.json (colonisé)';

--Vérification de la création des tables
SHOW TABLES IN SCHEMA BRONZE;

SELECT
  ' 11 tables BRONZE créées (9 CSV + 2 JSON colonisés)' AS status,
  COUNT(*) AS nb_tables
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = CURRENT_DATABASE()
  AND TABLE_SCHEMA  = 'BRONZE';


-- ==============================================================================================================================================================
-- Étape 3 – Chargement des données
-- ==============================================================================================================================================================

USE ROLE SYSADMIN;
USE DATABASE BERBERE_LAB;
USE SCHEMA BRONZE;
USE WAREHOUSE WH_BRB_LAB;


-- File CSV : virgule, 1 header, guillemets facultatifs, trim, NULLs
CREATE OR REPLACE FILE FORMAT BRONZE.FF_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('', 'NULL');

-- File JSON : fichiers sous forme de tableaux d’objets
CREATE OR REPLACE FILE FORMAT BRONZE.FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

--Chargement de données CSV
-- 1. customer_demographics.csv
COPY INTO BRONZE.customer_demographics
FROM @BRONZE.STG_FOOD_BEVERAGE/customer_demographics.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 2. customer_service_interactions.csv
COPY INTO BRONZE.customer_service_interactions
FROM @BRONZE.STG_FOOD_BEVERAGE/customer_service_interactions.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 3. financial_transactions.csv
COPY INTO BRONZE.financial_transactions
FROM @BRONZE.STG_FOOD_BEVERAGE/financial_transactions.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 4. promotions-data.csv
COPY INTO BRONZE.promotions_data
FROM @BRONZE.STG_FOOD_BEVERAGE/promotions-data.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 5. marketing_campaigns.csv
COPY INTO BRONZE.marketing_campaigns
FROM @BRONZE.STG_FOOD_BEVERAGE/marketing_campaigns.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 6. product_reviews.csv
COPY INTO BRONZE.product_reviews
FROM @BRONZE.STG_FOOD_BEVERAGE/product_reviews.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 7. logistics_and_shipping.csv
COPY INTO BRONZE.logistics_and_shipping
FROM @BRONZE.STG_FOOD_BEVERAGE/logistics_and_shipping.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 8. supplier_information.csv
COPY INTO BRONZE.supplier_information
FROM @BRONZE.STG_FOOD_BEVERAGE/supplier_information.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';

-- 9. employee_records.csv
COPY INTO BRONZE.employee_records
FROM @BRONZE.STG_FOOD_BEVERAGE/employee_records.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
ON_ERROR = 'CONTINUE';


--Chergement de données JSON 
-- 10. inventory.json  -> table colonisée BRONZE.inventory
COPY INTO BRONZE.inventory
  (product_id, product_category, region, country, warehouse,
   current_stock, reorder_point, lead_time, last_restock_date)
FROM (
  SELECT
    $1:product_id::string,
    $1:product_category::string,
    $1:region::string,
    $1:country::string,
    $1:warehouse::string,
    $1:current_stock::number,
    $1:reorder_point::number,
    $1:lead_time::number,
    TO_DATE($1:last_restock_date::string)
  FROM @BRONZE.STG_FOOD_BEVERAGE/inventory.json
)
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_JSON)
ON_ERROR = 'CONTINUE';

-- 11. store_locations.json -> table colonisée BRONZE.store_locations
COPY INTO BRONZE.store_locations
  (store_id, store_name, store_type, region, country, city, address,
   postal_code, square_footage, employee_count)
FROM (
  SELECT
    $1:store_id::string,
    $1:store_name::string,
    $1:store_type::string,
    $1:region::string,
    $1:country::string,
    $1:city::string,
    $1:address::string,
    -- postal_code parfois numérique : on force en texte
    TO_VARCHAR($1:postal_code),
    $1:square_footage::float,
    $1:employee_count::number
  FROM @BRONZE.STG_FOOD_BEVERAGE/store_locations.json
)
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_JSON)
ON_ERROR = 'CONTINUE';

--Premiere vérification du chargement des données

SELECT 'customer_demographics'      AS table_name, COUNT(*) AS rows_cnt FROM BRONZE.customer_demographics UNION ALL
SELECT 'customer_service_interactions', COUNT(*)            FROM BRONZE.customer_service_interactions UNION ALL
SELECT 'financial_transactions',     COUNT(*)               FROM BRONZE.financial_transactions UNION ALL
SELECT 'promotions_data',            COUNT(*)               FROM BRONZE.promotions_data UNION ALL
SELECT 'marketing_campaigns',        COUNT(*)               FROM BRONZE.marketing_campaigns UNION ALL
SELECT 'product_reviews',            COUNT(*)               FROM BRONZE.product_reviews UNION ALL
SELECT 'logistics_and_shipping',     COUNT(*)               FROM BRONZE.logistics_and_shipping UNION ALL
SELECT 'supplier_information',       COUNT(*)               FROM BRONZE.supplier_information UNION ALL
SELECT 'employee_records',           COUNT(*)               FROM BRONZE.employee_records UNION ALL
SELECT 'inventory',                  COUNT(*)               FROM BRONZE.inventory UNION ALL
SELECT 'store_locations',            COUNT(*)               FROM BRONZE.store_locations
ORDER BY table_name;

--On remarque sur les résultats que la table "product_reviews" est vide 

--Correction d'anomalies
COPY INTO BRONZE.product_reviews
FROM @BRONZE.STG_FOOD_BEVERAGE/product_reviews.csv
FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV)
VALIDATION_MODE = 'RETURN_ERRORS';

--Creation tsv file pour product_reviews

CREATE OR REPLACE FILE FORMAT BRONZE.FF_TSV_REVIEWS
  TYPE = CSV
  FIELD_DELIMITER = '\x09'      -- ← TAB (ASCII 0x09). Alternative : '\011'
  RECORD_DELIMITER = '\n'       -- ou '\r\n' si ton fichier est Windows-CRLF
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE
  TRIM_SPACE = TRUE
  NULL_IF = ('', 'NULL')
  -- Optionnel en mode “tolérant” :
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
--affichage des colones 
SELECT t.$1 AS review_id,
       t.$2 AS product_id,
       t.$3 AS reviewer_id,
       t.$4 AS reviewer_name,
       t.$5 AS rating,
       t.$6 AS review_date,
       t.$7 AS review_title,
       t.$8 AS review_text,
       t.$9 AS product_category
FROM @BRONZE.STG_FOOD_BEVERAGE/product_reviews.csv
     (FILE_FORMAT => BRONZE.FF_TSV_REVIEWS) AS t
LIMIT 5;

-- On cible les colonnes attendues par la table BRONZE.product_reviews
COPY INTO BRONZE.product_reviews
  (review_id, product_id, reviewer_id, reviewer_name,
   rating, review_date, review_title, review_text, product_category)
FROM (
  SELECT
    TRY_TO_NUMBER($1)                          AS review_id,
    $2::STRING                                 AS product_id,
    $3::STRING                                 AS reviewer_id,
    $4::STRING                                 AS reviewer_name,
    TRY_TO_NUMBER($5)                          AS rating,
    -- Format le plus fréquent 'YYYY-MM-DD HH24:MI:SS' → on prend la date
    TO_DATE(SUBSTR($6::STRING, 1, 10))         AS review_date,
    $7::STRING                                 AS review_title,
    $8::STRING                                 AS review_text,
    $9::STRING                                 AS product_category
  FROM @BRONZE.STG_FOOD_BEVERAGE/product_reviews.csv
       (FILE_FORMAT => BRONZE.FF_TSV_REVIEWS)
)
ON_ERROR = 'CONTINUE';

--Verification
SELECT COUNT(*) AS rows_reviews FROM BRONZE.product_reviews;

SELECT * FROM BRONZE.product_reviews LIMIT 10;

--Table bien chargé avec 993 lignes


-- Compte brut des enregistrements *lisibles* via le format TSV
SELECT COUNT(*) AS rows_in_stage
FROM @BRONZE.STG_FOOD_BEVERAGE/product_reviews.csv
     (FILE_FORMAT => BRONZE.FF_TSV_REVIEWS);

--On voit bien qu'on a téléchargé toutes les lignes du stage


-- ==============================================================================================================================================================
-- Étape 4 – Vérifications
-- ==============================================================================================================================================================
--vérification de nombre des lignes 
SELECT 'customer_demographics'      AS table_name, COUNT(*) AS rows_cnt FROM BRONZE.customer_demographics UNION ALL
SELECT 'customer_service_interactions', COUNT(*)            FROM BRONZE.customer_service_interactions UNION ALL
SELECT 'financial_transactions',     COUNT(*)               FROM BRONZE.financial_transactions UNION ALL
SELECT 'promotions_data',            COUNT(*)               FROM BRONZE.promotions_data UNION ALL
SELECT 'marketing_campaigns',        COUNT(*)               FROM BRONZE.marketing_campaigns UNION ALL
SELECT 'product_reviews',            COUNT(*)               FROM BRONZE.product_reviews UNION ALL
SELECT 'logistics_and_shipping',     COUNT(*)               FROM BRONZE.logistics_and_shipping UNION ALL
SELECT 'supplier_information',       COUNT(*)               FROM BRONZE.supplier_information UNION ALL
SELECT 'employee_records',           COUNT(*)               FROM BRONZE.employee_records UNION ALL
SELECT 'inventory',                  COUNT(*)               FROM BRONZE.inventory UNION ALL
SELECT 'store_locations',            COUNT(*)               FROM BRONZE.store_locations
ORDER BY table_name;

--Inspection des échantillons
SELECT * FROM BRONZE.promotions_data LIMIT 10;
SELECT * FROM BRONZE.product_reviews LIMIT 10;
SELECT * FROM BRONZE.inventory LIMIT 10;





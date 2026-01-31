-- ===================================================================================================================================================
-- Phase 3 – Data Product et ML
-- ===================================================================================================================================================


USE ROLE SYSADMIN;
USE DATABASE BERBERE_LAB;
USE WAREHOUSE WH_BRB_LAB;
USE SCHEMA ANALYTICS;

-- Table ANALYTICS.ventes_enrichies
CREATE OR REPLACE TABLE ANALYTICS.ventes_enrichies AS
SELECT
    f.transaction_id,                                    -- PK logique
    f.transaction_date,
    /* Découpage temporel simple (utilisable en BI/ML) */
    YEAR(f.transaction_date)        AS annee,
    MONTH(f.transaction_date)       AS mois,
    TO_CHAR(f.transaction_date, 'YYYY-MM') AS mois_aaaamm,
    DAYOFWEEKISO(f.transaction_date)      AS jour_semaine_iso,
    IFF(DAYOFWEEKISO(f.transaction_date) IN (6,7), 1, 0) AS is_weekend,
    /* Métriques/Dimensions */
    f.transaction_type,
    f.amount,
    f.payment_method,
    f.entity,
    f.region,
    f.account_code
FROM SILVER.financial_transactions_clean f
WHERE UPPER(f.transaction_type) = 'SALE';

-- Documentation table & colonnes
ALTER TABLE ANALYTICS.ventes_enrichies SET COMMENT = 'Ventes enrichies (transactions de type SALE) prêtes pour analyses.';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN transaction_id   COMMENT 'Identifiant unique de transaction (clé de jointure)';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN transaction_date COMMENT 'Date de la transaction';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN annee            COMMENT 'Année de la vente';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN mois             COMMENT 'Mois (numérique 1-12)';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN mois_aaaamm      COMMENT 'Mois format YYYY-MM';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN jour_semaine_iso COMMENT 'Jour de semaine ISO (1=Lundi .. 7=Dimanche)';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN is_weekend       COMMENT 'Flag week-end (1 si samedi/dimanche)';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN transaction_type COMMENT 'Type de transaction (filtré sur SALE)';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN amount           COMMENT 'Montant (>=0 en SILVER)';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN payment_method   COMMENT 'Méthode de paiement';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN entity           COMMENT 'Entité/point d’encaissement';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN region           COMMENT 'Région (dimension de jointure)';
ALTER TABLE ANALYTICS.ventes_enrichies MODIFY COLUMN account_code     COMMENT 'Compte comptable';

-- Clé primaire logique (non-enforcée par Snowflake) pour la documentation
ALTER TABLE ANALYTICS.ventes_enrichies
  ADD CONSTRAINT PK_VENTES_ENRICHIES PRIMARY KEY (transaction_id);

-- Contrôles rapides
-- 1) Unicité de transaction
SELECT COUNT(*) AS nb, COUNT(DISTINCT transaction_id) AS nb_distinct
FROM ANALYTICS.ventes_enrichies;

-- 2) Montants négatifs (devrait être 0 après SILVER)
SELECT COUNT(*) AS nb_negatifs
FROM ANALYTICS.ventes_enrichies
WHERE amount < 0;

select count(*) as nb from ventes_enrichies 

-- Table ANALYTICS.promotions_actives

CREATE OR REPLACE TABLE ANALYTICS.promotions_actives AS
SELECT
    p.promotion_id,
    p.product_category,
    p.promotion_type,
    p.discount_percentage,          -- [0,1] validé en SILVER
    p.start_date,
    p.end_date,
    p.region,
    /* Flags & dérivés */
    IFF(CURRENT_DATE() BETWEEN p.start_date AND COALESCE(p.end_date, '9999-12-31'::DATE), 1, 0) AS is_active,
    DATEDIFF('day', p.start_date, COALESCE(p.end_date, CURRENT_DATE()))                          AS duree_jours
FROM SILVER.promotions_clean p;  -- <-- plus de WHERE ici

-- (Documentation & contraintes inchangées)
ALTER TABLE ANALYTICS.promotions_actives SET COMMENT = 'Promotions avec flag is_active (non filtrées).';
ALTER TABLE ANALYTICS.promotions_actives
  ADD CONSTRAINT PK_PROMOTIONS_ACTIVES PRIMARY KEY (promotion_id);

--Verification
select * from ANALYTICS.promotions_actives limit 10
select * from ventes_enrichies limit 10


-- Table ANALYTICS.clients_enrichis
CREATE OR REPLACE TABLE ANALYTICS.clients_enrichis AS
SELECT
    c.customer_id,                              -- PK logique
    c.name,
    c.date_of_birth,
    DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) AS age,
    CASE
        WHEN c.date_of_birth IS NULL THEN 'NA'
        WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 25 THEN '18-24'
        WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 35 THEN '25-34'
        WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 45 THEN '35-44'
        WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 60 THEN '45-59'
        ELSE '60+'
    END AS age_segment,
    c.gender,
    c.marital_status,
    c.annual_income,
    CASE
        WHEN c.annual_income IS NULL THEN 'NA'
        WHEN c.annual_income < 20000 THEN '<20k'
        WHEN c.annual_income < 50000 THEN '20k-49k'
        WHEN c.annual_income < 100000 THEN '50k-99k'
        ELSE '100k+'
    END AS income_bracket,
    c.region,
    c.country,
    c.city
FROM SILVER.customer_demographics_clean c;

-- Documentation
ALTER TABLE ANALYTICS.clients_enrichis SET COMMENT = 'Clients enrichis avec dérivés (âge, segment d’âge, income bracket).';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN customer_id    COMMENT 'Identifiant client (clé de jointure)';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN name           COMMENT 'Nom client';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN date_of_birth  COMMENT 'Date de naissance (source SILVER)';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN age            COMMENT 'Âge (année entière, calculée à date du jour)';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN age_segment    COMMENT 'Segment d’âge simple';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN gender         COMMENT 'Genre';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN marital_status COMMENT 'Statut marital';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN annual_income  COMMENT 'Revenu annuel (>=0 en SILVER)';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN income_bracket COMMENT 'Tranche de revenu (règles simples)';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN region         COMMENT 'Région';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN country        COMMENT 'Pays';
ALTER TABLE ANALYTICS.clients_enrichis MODIFY COLUMN city           COMMENT 'Ville';

ALTER TABLE ANALYTICS.clients_enrichis
  ADD CONSTRAINT PK_CLIENTS_ENRICHIS PRIMARY KEY (customer_id);

-- Contrôles rapides
-- 1) Unicité client
SELECT COUNT(*) AS nb, COUNT(DISTINCT customer_id) AS nb_distinct
FROM ANALYTICS.clients_enrichis;

-- 2) Ages hors bornes (sanity check)
SELECT
  SUM(IFF(age < 0 OR age > 120, 1, 0)) AS nb_ages_incoherents
FROM ANALYTICS.clients_enrichis;

--Verification
select * from clients_enrichis limit 10
# Projet Architecture Big Data "Berbère Lab"
## VObjectif du projet

Le projet couvre l'ensemble de la chaîne de valeur : ingestion des données, nettoyage, analyses exploratoires et business, visualisations interactives.


## Architecture Technique

### Stack technologique
- **Plateforme Cloud** : Snowflake
- **Langage SQL** : Snowflake SQL
- **Dashboards** : Streamlit (natif Snowflake)
- **Warehouse** : WH_BRB_LAB (XSMALL, auto-suspend 60s)

### Structure de la base de données

```
BERBERE_LAB (Database)
│
├── BRONZE (Schema)                    # Données brutes
│   ├── customer_demographics           # CSV - 9 tables CSV
│   ├── customer_service_interactions   # CSV
│   ├── financial_transactions          # CSV
│   ├── promotions_data                 # CSV
│   ├── marketing_campaigns             # CSV
│   ├── product_reviews                 # CSV
│   ├── logistics_and_shipping          # CSV
│   ├── supplier_information            # CSV
│   ├── employee_records                # CSV
│   ├── inventory                       # JSON (colonisé)
│   └── store_locations                 # JSON (colonisé)
│
└── SILVER (Schema)                    # Données nettoyées
    ├── customer_demographics_clean
    ├── customer_service_interactions_clean
    ├── financial_transactions_clean
    ├── promotions_clean
    ├── marketing_campaigns_clean
    ├── product_reviews_clean
    ├── logistics_and_shipping_clean
    ├── supplier_information_clean
    ├── employee_records_clean
    ├── inventory_clean
    └── store_locations_clean
```

---

##  Organisation des fichiers

### Scripts SQL
### load_data.sql
 **setup_snowflake**
   - Création de la database BERBERE_LAB
   - Création des schémas BRONZE et SILVER
   - Configuration du warehouse WH_BRB_LAB
   - Création du stage S3 (STG_FOOD_BEVERAGE)

 **Create_table**
   - Définition des 11 tables dans le schéma BRONZE
   - Typage des colonnes avec PRIMARY KEY

 **Chargement de données**
   - Chargement des données depuis S3 vers BRONZE

### clean_data.sql
   - **Nettoyage BRONZE → SILVER** avec :
     - Gestion des doublons (ROW_NUMBER)
     - Traitement des valeurs nulles (TRIM, NULLIF)
     - Validation des données (montants ≥ 0, notes ∈ [1-5])
     - Cohérence des dates (start_date ≤ end_date)


### Compréhension_des_jeux_de_données.sql
Analyse de la qualité et profil des données :

**Vue d'ensemble par table** : volumétrie, périodes couvertes, clés distinctes

![alt text](image.png)

**Analyse de qualité** : identification des valeurs manquantes par colonne

**Distributions** : analyse par dimension selon la nature de chaque table
- Géographique (région, pays)
- Démographique (genre, âge)
- Temporelle (périodes d'activité)

### 5. Analyses_exploratoires_descriptives.sql
Analyses descriptives sur les ventes et clients :

**Évolution temporelle des ventes**

Par mois :
![alt text](image-1.png)

Par trimestre :
![alt text](image-2.png)

Par jour de la semaine :
![alt text](image-3.png)

**Performance géographique**

![alt text](image-4.png)

**Segmentation clients**

Par tranche d'âge :
![alt text](image-5.png)

Par niveau de revenu :
![alt text](image-6.png)

#### 6. Analyses_business_transverses.sql
Analyses métier croisées sur 4 axes :

**2.3.1 - Ventes et Promotions**

Impact des promotions sur les ventes :
![alt text](image-7.png)

Sensibilité des catégories aux promotions :
![alt text](image-8.png)

**2.3.2 - Marketing ROI**

Performance des campagnes marketing :
![alt text](image-9.png)

Efficacité par public cible :
![alt text](image-10.png)

**2.3.3 - Expérience Client**

Distribution des avis produits :
![alt text](image-11.png)

Satisfaction par type d'interaction :
![alt text](image-12.png)

**2.3.4 - Opérations et Logistique**

Ruptures de stock par catégorie :
![alt text](image-13.png)

Performance logistique :
![alt text](image-14.png)

Analyse des retards potentiels :
![alt text](image-15.png)

---

## Dashboards Streamlit

### 1. sales_dashboard.py

**Objectif** : Suivi en temps réel des performances commerciales avec filtres interactifs.

**KPIs principaux**
- Chiffre d'affaires total
- Nombre de transactions
- Panier moyen
- Meilleur mois

**Visualisations**
- Évolution mensuelle des ventes
- Performance par région
- Performance par jour de la semaine

**Filtres disponibles**
- Régions géographiques (multi-sélection)
- Modes de paiement
- Montant minimum de transaction

**Fonctionnalités**
- Export CSV des données filtrées
- Tableaux détaillés expandables
- Comparaison Top 3 régions
- Bouton réinitialisation des filtres

---

### 2. marketing_roi.py

**Objectif** : Évaluation de l'efficacité des investissements marketing.

**KPIs principaux**
- Budget total investi
- Ventes générées
- ROI moyen (ratio ventes/budget)
- Portée totale

**Sections d'analyse**

**Top 10 campagnes par ROI** : identification des campagnes les plus rentables

**Performance par type de campagne** : comparaison Email, Social Media, Influencer, Display, etc.

**Performance par public cible** : analyse de la réactivité par segment (Millennials, Familles, Professionnels)

**Alertes automatiques**
- Message d'alerte si ROI < 1 (perte financière)
- Indicateur du budget total concerné

**Recommandations stratégiques**
- Actions à renforcer (campagnes performantes)
- Axes d'amélioration (tests et optimisations)
- Actions à stopper (campagnes non rentables)

**Tableau détaillé** : vue exhaustive de toutes les campagnes avec métriques complètes

---

### 3. promotion_analysis.py

**Objectif** : Analyse de l'impact des stratégies promotionnelles sur les ventes.

**Métriques analysées**
- Impact global des promotions
- Performance par région
- Sensibilité par catégorie produit
- Distribution des niveaux de remise

**Filtres disponibles**
- Régions géographiques
- Catégories de produits
- Types de promotions
- Plage de niveau de remise (slider 0-100%)

**Recommandations dynamiques**

Le dashboard génère des alertes contextuelles :
- Si remise moyenne > 35% : alerte sur l'impact marge
- Si durée moyenne > 20 jours : attention à la banalisation

**Export de données**
- Analyse par catégorie
- Analyse par région
- Distribution des remises

Tous les exports sont au format CSV avec horodatage.

---

## Synthèse des analyses réalisées

### Analyses Exploratoires

**Dimension Ventes**
- Évolution mensuelle et trimestrielle
- Croissance MoM (Month-over-Month)
- Saisonnalité hebdomadaire
- Performance géographique par région

**Dimension Clients**
- Segmentation démographique (âge, genre, revenu)
- Distribution géographique
- Profils socio-économiques

**Dimension Produits**
- Classement par volume d'avis
- Distribution des notes (échelle 1-5)
- Catégories les plus plébiscitées

### Analyses Business Transverses

**Marketing et Promotions**
- ROI Marketing : ratio ventes générées / budget investi
- Impact Promotions : comparaison ventes avec/sans promotion
- Efficacité par canal : taux de conversion par type de campagne
- Sensibilité produits : élasticité aux remises par catégorie

**Expérience Client**
- Satisfaction service : analyse par type d'interaction et catégorie de problème
- Corrélation durée/satisfaction : impact du temps de résolution
- Performance produits : relation entre notation et volume d'avis

**Opérations et Logistique**
- Ruptures de stock : taux par catégorie et entrepôt
- Performance livraison : délais par méthode d'expédition et région
- Coûts logistiques : analyse comparative par transporteur

---

## Auteur

Projet réalisé dans le cadre du cours Architecture Big Data par:
 **FOUAISSI Mohamed Fares**
 **AMIEL Augustin**
 **ACHOURI Abdenour**

# Berbere Lab - Projet d'Analytics Food & Beverage

## Vue d'ensemble du projet

Projet d'analyse de données end-to-end pour une entreprise du secteur Food & Beverage, implémenté sur **Snowflake** avec des dashboards **Streamlit**. 

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

1. **setup_snowflake.sql**
   - Création de la database BERBERE_LAB
   - Création des schémas BRONZE et SILVER
   - Configuration du warehouse WH_BRB_LAB
   - Création du stage S3 (STG_FOOD_BEVERAGE)

2. **Create_table.sql**
   - Définition des 11 tables dans le schéma BRONZE
   - Typage des colonnes avec PRIMARY KEY

3. **Load_data.sql**
   - Chargement des données depuis S3 vers BRONZE
   - **Nettoyage BRONZE → SILVER** avec :
     - Gestion des doublons (ROW_NUMBER)
     - Traitement des valeurs nulles (TRIM, NULLIF)
     - Validation des données (montants ≥ 0, notes ∈ [1-5])
     - Cohérence des dates (start_date ≤ end_date)

4. **Compréhension_des_jeux_de_données.sql**
   - Vue d'ensemble de chaque table (volumétrie, périodes)
   ![alt text](image.png)
   - Analyse de la qualité des données( valeurs manquantes)
   - Distributions et profils par table(pour chaque tables on a fait une analyse sur les distribution geographique, par genre, par periode tout depend de la table)

5. **Analyses_exploratoires_descriptives.sql**
   - Évolution temporelle des ventes (mensuelle, trimestrielle, hebdomadaire)
   Par mois:
   ![alt text](image-1.png)
   Par trimestre:
   ![alt text](image-2.png)
   Par jour de la semaine:
   ![alt text](image-3.png)

   - Performance par région
   ![alt text](image-4.png)
   - Segmentation clients (âge, revenu, géographie)
   Par age:
   ![alt text](image-5.png)
   Par revenu
   ![alt text](image-6.png)


6. **Analyses_business_transverses.sql**
   - **2.3.1** - Ventes et Promotions (impact, sensibilité par catégorie)
   impact:
   ![alt text](image-7.png)
   sensibilité:
   ![alt text](image-8.png)

   - **2.3.2** - Marketing ROI (performance des campagnes)
   Les compagne les plus éficaces :
   ![alt text](image-9.png)
   Eficacité par public cible:
   ![alt text](image-10.png)


   - **2.3.3** - Expérience Client (avis produits, satisfaction service)
   Avis produit: 
   ![alt text](image-11.png)
   Satisfaction par type de transaction
   ![alt text](image-12.png)


   - **2.3.4** - Opérations (ruptures de stock, logistique)
   Rupture par categorie:
   ![alt text](image-13.png)
   logistique:
   ![alt text](image-14.png)
   Analyse des retard potentiel:
   ![alt text](image-15.png)

### Dashboards Streamlit

1. **sales_dashboard.py**
#  Dashboard de Ventes - Guide d'utilisation

## C'est quoi ce dashboard ?

Un tableau de bord interactif pour suivre les ventes de l'entreprise. L'idée c'est de pouvoir rapidement voir ce qui marche, ce qui marche moins, et où concentrer ses efforts.


### Les KPIs en haut
- Le chiffre d'affaires total
- Le nombre de transactions
- Le panier moyen
- Le meilleur mois

### Les graphiques
1. **Évolution mensuelle** : Pour voir la tendance sur l'année
2. **Performance par région** : Qui cartonne, qui rame
3. **Performance par jour de la semaine** : Tes meilleurs jours de vente

### Les filtres (à gauche)
- **Régions** : Focus sur une zone géographique
- **Modes de paiement** : Carte, espèces, etc.
- **Montant minimum** : Pour exclure les petites transactions qui polluent l'analyse


##  Export des données
- Les ventes mensuelles
- Les ventes par région
- La performance hebdomadaire

Tout en CSV, prêt pour Excel ou Google Sheets.

##  Réinitialiser

Il y'a un bouton "Réinitialiser" dans la sidebar qui remet tout à zéro.


2. **marketing_roi.py**
## C'est quoi ce dashboard ?

Un outil pour savoir si le budget marketing est bien investi cad on voit combien on dépense, combien ça rapporte, et où le ROI est bon (ou pas).

## Ce que ça fait

### Les KPIs 
Direct, tu vois :
- Le budget total dépensé
- Les ventes générées par ces campagnes
- Le ROI moyen (combien tu gagnes pour 1€ investi)
- La portée totale (combien de personnes touchées)



### Les sections principales

**1. Top 10 campagnes**
Les meilleures campagnes par ROI. C'est là que tu vois ce qui marche vraiment. Si une campagne a un ROI de 8x, on sait qu'il faut la répliquer.

**2. Performance par type**
Email, Social Media, Influencer, Display... Chaque type de campagne a son graphique. On voit direct quel canal convertit le mieux et lequel coûte trop cher pour rien.

**3. Performance par public cible**
Millennials, Familles, Pros... Quel public réagit le mieux ? 

## 🚨 Les alertes

En bas, si on a des campagnes avec un ROI < 1 (tu perds de l'argent), un gros message rouge apparaît.

**ROI < 1** = on dépense 100€, on récupère 80€. Mauvais deal.

## 💡 Les recommandations

Trois colonnes de conseils :
- **Vert** : Ce qu'il faut faire (renforcer ce qui marche)
- **Bleu** : Comment s'améliorer (tester de nouvelles choses)
- **Orange** : Ce qu'il faut arrêter (couper ce qui ne marche pas)

## Le tableau détaillé

Tout en bas, on a le détail de chaque campagne :
- Nom de la campagne
- Type
- Région
- Budget
- Ventes générées
- ROI
- Taux de conversion



3. **promotion_analysis.py**
## C'est quoi ce dashboard ?

Un dashboard pour comprendre si les promotions fonctionnent vraiment. Est-ce qu'elles boostent les ventes ? Lesquelles marchent le mieux ? Où ? C'est quoi le bon niveau de remise ?

### Les KPI
  - Impact global
  - Performance par région
  - Sensibilité par catégorie
  - Distribution des remises
  
### Les filtres utiles

 **Régions
 **Catégories de produits
 **Types de promotions
 **Niveau de remise


### Les recommandations

En bas, y'a une section qui donne des conseils basés sur tes données :
- Si une remise moyenne dépasse 35% → Alerte rouge sur la marge
- Si une promos durent plus de 20 jours → Attention à la banalisation

## Export

 Trois boutons en bas pour télécharger tout en CSV.


##  Analyses réalisées

### 1. Analyses Exploratoires

#### Ventes
- Évolution mensuelle/trimestrielle
- Croissance MoM (Month-over-Month)
- Saisonnalité (jour de semaine)
- Performance géographique

#### Clients
- Segmentation démographique (âge, genre, revenu)
- Distribution géographique
- Profils de revenus

#### Produits
- Top produits par volume d'avis
- Distribution des notes (1-5 étoiles)
- Catégories les plus populaires

### 2. Analyses Business Transverses

#### Marketing & Promotions
- **ROI Marketing** : Ventes générées / Budget investi
- **Impact Promotions** : Comparaison ventes avec/sans promo
- **Efficacité par canal** : Conversion par type de campagne
- **Sensibilité produits** : Réponse aux remises par catégorie

#### Expérience Client
- **Satisfaction service** : Par type d'interaction et catégorie problème
- **Corrélation durée/satisfaction** : Impact du temps de résolution
- **Performance produits** : Lien entre notes et volume d'avis

#### Opérations & Logistique
- **Ruptures de stock** : Taux par catégorie et entrepôt
- **Performance livraison** : Délais par méthode et région
- **Coûts logistiques** : Analyse par transporteur

---


## 👨‍💻 Auteur
Projet réalisé dans le cadre du lab Food & Beverage sur Snowflake

## 📅 Dernière mise à jour
Janvier 2026

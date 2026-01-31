# Analyse des Promotions - Berbere Lab
# Streamlit natif Snowflake (AVEC filtres interactifs)

import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime

# --------------------------------------------------
# Configuration
# --------------------------------------------------
st.set_page_config(
    page_title="Analyse Promotions",
    page_icon="🎯",
    layout="wide"
)

session = get_active_session()

st.title("🎯 Analyse des Promotions")
st.markdown("### Impact des stratégies promotionnelles sur les ventes")
st.markdown("---")

# --------------------------------------------------
# Fonctions de chargement des filtres
# --------------------------------------------------
@st.cache_data
def get_distinct_regions():
    """Récupère les régions disponibles"""
    return session.sql("""
        SELECT DISTINCT region
        FROM BERBERE_LAB.SILVER.promotions_clean
        WHERE region IS NOT NULL
        ORDER BY region
    """).to_pandas()['REGION'].tolist()

@st.cache_data
def get_distinct_categories():
    """Récupère les catégories de produits"""
    return session.sql("""
        SELECT DISTINCT product_category
        FROM BERBERE_LAB.SILVER.promotions_clean
        WHERE product_category IS NOT NULL
        ORDER BY product_category
    """).to_pandas()['PRODUCT_CATEGORY'].tolist()

@st.cache_data
def get_distinct_promo_types():
    """Récupère les types de promotions"""
    return session.sql("""
        SELECT DISTINCT promotion_type
        FROM BERBERE_LAB.SILVER.promotions_clean
        WHERE promotion_type IS NOT NULL
        ORDER BY promotion_type
    """).to_pandas()['PROMOTION_TYPE'].tolist()

# --------------------------------------------------
# Fonctions de chargement des données avec filtres
# --------------------------------------------------
@st.cache_data
def load_promo_impact(regions, categories, min_discount, max_discount):
    """Impact global des promotions avec filtres"""
    if not regions or not categories:
        return None
    
    regions_sql = ",".join([f"'{r.replace(chr(39), chr(39)+chr(39))}'" for r in regions])
    categories_sql = ",".join([f"'{c.replace(chr(39), chr(39)+chr(39))}'" for c in categories])
    
    return session.sql(f"""
        WITH sales_promo_overlap AS (
            SELECT
                ft.amount,
                ft.region,
                p.product_category,
                p.discount_percentage,
                CASE 
                    WHEN p.promotion_id IS NOT NULL THEN 'Avec promotion'
                    ELSE 'Sans promotion'
                END AS promo_status
            FROM BERBERE_LAB.SILVER.financial_transactions_clean ft
            LEFT JOIN BERBERE_LAB.SILVER.promotions_clean p
                ON ft.region = p.region
                AND ft.transaction_date BETWEEN p.start_date AND p.end_date
                AND p.region IN ({regions_sql})
                AND p.product_category IN ({categories_sql})
                AND p.discount_percentage BETWEEN {min_discount} AND {max_discount}
            WHERE ft.transaction_type = 'Sale'
        )
        SELECT
            promo_status,
            COUNT(*) AS nb_transactions,
            SUM(amount) AS total_sales,
            AVG(amount) AS avg_transaction_value
        FROM sales_promo_overlap
        GROUP BY promo_status
        ORDER BY total_sales DESC
    """).to_pandas()

@st.cache_data
def load_category_sensitivity(regions, categories, promo_types, min_discount, max_discount):
    """Sensibilité des catégories avec filtres"""
    if not regions or not categories or not promo_types:
        return None
    
    # Échapper les apostrophes en les doublant
    regions_sql = ",".join([f"'{r.replace(chr(39), chr(39)+chr(39))}'" for r in regions])
    categories_sql = ",".join([f"'{c.replace(chr(39), chr(39)+chr(39))}'" for c in categories])
    types_sql = ",".join([f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in promo_types])
    
    return session.sql(f"""
        SELECT
            product_category,
            region,
            COUNT(DISTINCT promotion_id) AS nb_promotions,
            AVG(discount_percentage) * 100 AS avg_discount_pct,
            MIN(discount_percentage) * 100 AS min_discount_pct,
            MAX(discount_percentage) * 100 AS max_discount_pct,
            AVG(DATEDIFF(day, start_date, end_date)) AS avg_promo_duration_days
        FROM BERBERE_LAB.SILVER.promotions_clean
        WHERE product_category IN ({categories_sql})
          AND region IN ({regions_sql})
          AND promotion_type IN ({types_sql})
          AND discount_percentage BETWEEN {min_discount} AND {max_discount}
        GROUP BY product_category, region
        ORDER BY nb_promotions DESC
    """).to_pandas()

@st.cache_data
def load_discount_ranges(regions, categories, promo_types):
    """Distribution des remises avec filtres"""
    if not regions or not categories or not promo_types:
        return None
    
    regions_sql = ",".join([f"'{r.replace(chr(39), chr(39)+chr(39))}'" for r in regions])
    categories_sql = ",".join([f"'{c.replace(chr(39), chr(39)+chr(39))}'" for c in categories])
    types_sql = ",".join([f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in promo_types])
    
    return session.sql(f"""
        SELECT
            CASE
                WHEN discount_percentage < 0.10 THEN '< 10%'
                WHEN discount_percentage < 0.20 THEN '10-19%'
                WHEN discount_percentage < 0.30 THEN '20-29%'
                WHEN discount_percentage < 0.40 THEN '30-39%'
                ELSE '40%+'
            END AS discount_range,
            COUNT(*) AS nb_promotions,
            SUM(DATEDIFF(day, start_date, end_date)) AS total_days
        FROM BERBERE_LAB.SILVER.promotions_clean
        WHERE discount_percentage IS NOT NULL
          AND region IN ({regions_sql})
          AND product_category IN ({categories_sql})
          AND promotion_type IN ({types_sql})
        GROUP BY discount_range
        ORDER BY discount_range
    """).to_pandas()

@st.cache_data
def load_promo_by_region(regions, categories, promo_types, min_discount, max_discount):
    """Promotions par région"""
    if not regions or not categories or not promo_types:
        return None
    
    regions_sql = ",".join([f"'{r.replace(chr(39), chr(39)+chr(39))}'" for r in regions])
    categories_sql = ",".join([f"'{c.replace(chr(39), chr(39)+chr(39))}'" for c in categories])
    types_sql = ",".join([f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in promo_types])
    
    return session.sql(f"""
        SELECT
            region,
            COUNT(DISTINCT promotion_id) AS nb_promotions,
            AVG(discount_percentage) * 100 AS avg_discount_pct,
            COUNT(DISTINCT product_category) AS nb_categories
        FROM BERBERE_LAB.SILVER.promotions_clean
        WHERE region IN ({regions_sql})
          AND product_category IN ({categories_sql})
          AND promotion_type IN ({types_sql})
          AND discount_percentage BETWEEN {min_discount} AND {max_discount}
        GROUP BY region
        ORDER BY nb_promotions DESC
    """).to_pandas()

# --------------------------------------------------
# SIDEBAR - FILTRES
# --------------------------------------------------
st.sidebar.header("🎛️ Filtres")

# Charger les options
all_regions = get_distinct_regions()
all_categories = get_distinct_categories()
all_promo_types = get_distinct_promo_types()

# Filtres géographiques
st.sidebar.subheader("📍 Géographie")
selected_regions = st.sidebar.multiselect(
    "Régions",
    options=all_regions,
    default=all_regions,
    help="Sélectionnez les régions à analyser"
)

# Filtres produits
st.sidebar.subheader("📦 Produits")
selected_categories = st.sidebar.multiselect(
    "Catégories",
    options=all_categories,
    default=all_categories,
    help="Filtrer par catégorie de produits"
)

# Filtres promotions
st.sidebar.subheader("🎁 Types de Promotions")
selected_promo_types = st.sidebar.multiselect(
    "Types de promotions",
    options=all_promo_types,
    default=all_promo_types,
    help="Filtrer par type de promotion"
)

# Filtres remises
st.sidebar.subheader("💰 Niveau de Remise")
discount_range = st.sidebar.slider(
    "Plage de remise (%)",
    min_value=0,
    max_value=100,
    value=(0, 100),
    step=5,
    help="Filtrer par niveau de remise"
)

min_discount = discount_range[0] / 100
max_discount = discount_range[1] / 100

# Bouton reset
if st.sidebar.button("🔄 Réinitialiser les filtres"):
    st.rerun()

# --------------------------------------------------
# Chargement des données
# --------------------------------------------------
df_promo = load_promo_impact(selected_regions, selected_categories, min_discount, max_discount)
df_category = load_category_sensitivity(selected_regions, selected_categories, selected_promo_types, min_discount, max_discount)
df_discount = load_discount_ranges(selected_regions, selected_categories, selected_promo_types)
df_region = load_promo_by_region(selected_regions, selected_categories, selected_promo_types, min_discount, max_discount)

# --------------------------------------------------
# Vérification des données
# --------------------------------------------------
if df_promo is None or df_promo.empty:
    st.warning("⚠️ Aucune donnée disponible avec ces filtres. Veuillez ajuster vos critères.")
    st.stop()

# Afficher les filtres actifs
st.info(
    f"📊 **Filtres actifs** : {len(selected_regions)} région(s), "
    f"{len(selected_categories)} catégorie(s), "
    f"{len(selected_promo_types)} type(s) de promo, "
    f"remise {discount_range[0]}-{discount_range[1]}%"
)

# --------------------------------------------------
# Impact global
# --------------------------------------------------
st.header("📊 Impact Global des Promotions")

# Vérifier si les deux statuts existent
if len(df_promo) >= 2:
    with_promo = df_promo[df_promo["PROMO_STATUS"] == "Avec promotion"].iloc[0]
    without_promo = df_promo[df_promo["PROMO_STATUS"] == "Sans promotion"].iloc[0]

    col1, col2, col3 = st.columns(3)

    col1.metric("💰 Ventes AVEC promo", f"€{with_promo['TOTAL_SALES']:,.0f}")
    col2.metric("🛒 Panier AVEC promo", f"€{with_promo['AVG_TRANSACTION_VALUE']:.2f}")
    col3.metric("🧾 Transactions AVEC promo", f"{int(with_promo['NB_TRANSACTIONS']):,}")

    avg_diff = (
        (with_promo["AVG_TRANSACTION_VALUE"] /
         without_promo["AVG_TRANSACTION_VALUE"]) - 1
    ) * 100

    if avg_diff > 0:
        st.success(f"✅ Les promotions augmentent le panier moyen de **{avg_diff:.1f}%**")
    else:
        st.warning(f"⚠️ Les promotions réduisent le panier moyen de **{abs(avg_diff):.1f}%**")

    st.bar_chart(
        df_promo.set_index("PROMO_STATUS")[["TOTAL_SALES"]]
    )
else:
    st.warning("Données insuffisantes pour comparer avec/sans promotion")

st.markdown("---")

# --------------------------------------------------
# Performance par région
# --------------------------------------------------
if df_region is not None and not df_region.empty:
    st.header("🌍 Performance des Promotions par Région")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Nombre de promotions")
        st.bar_chart(df_region.set_index("REGION")[["NB_PROMOTIONS"]])
    
    with col2:
        st.subheader("Remise moyenne (%)")
        st.bar_chart(df_region.set_index("REGION")[["AVG_DISCOUNT_PCT"]])
    
    # Top 3 régions
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Top 3 - Nombre de promos**")
        top3_promos = df_region.nlargest(3, "NB_PROMOTIONS")[["REGION", "NB_PROMOTIONS"]]
        for idx, row in top3_promos.iterrows():
            st.metric(row["REGION"], f"{int(row['NB_PROMOTIONS'])}")
    
    with col2:
        st.markdown("**Top 3 - Remise moyenne**")
        top3_discount = df_region.nlargest(3, "AVG_DISCOUNT_PCT")[["REGION", "AVG_DISCOUNT_PCT"]]
        for idx, row in top3_discount.iterrows():
            st.metric(row["REGION"], f"{row['AVG_DISCOUNT_PCT']:.1f}%")
    
    with col3:
        st.markdown("**Top 3 - Catégories variées**")
        top3_categories = df_region.nlargest(3, "NB_CATEGORIES")[["REGION", "NB_CATEGORIES"]]
        for idx, row in top3_categories.iterrows():
            st.metric(row["REGION"], f"{int(row['NB_CATEGORIES'])} cat.")

st.markdown("---")

# --------------------------------------------------
# Analyse par catégorie
# --------------------------------------------------
if df_category is not None and not df_category.empty:
    st.header("📦 Sensibilité des Catégories aux Promotions")
    
    # Sélecteur de région
    category_region_filter = st.selectbox(
        "Région à afficher",
        options=["Toutes"] + selected_regions,
        key="category_region"
    )
    
    # Filtrer ou agréger
    if category_region_filter == "Toutes":
        df_category_display = df_category.groupby("PRODUCT_CATEGORY").agg({
            "NB_PROMOTIONS": "sum",
            "AVG_DISCOUNT_PCT": "mean",
            "AVG_PROMO_DURATION_DAYS": "mean"
        }).reset_index().sort_values("NB_PROMOTIONS", ascending=False)
    else:
        df_category_display = df_category[
            df_category["REGION"] == category_region_filter
        ].sort_values("NB_PROMOTIONS", ascending=False)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Nombre de promotions par catégorie")
        st.bar_chart(
            df_category_display.set_index("PRODUCT_CATEGORY")[["NB_PROMOTIONS"]]
        )

    with col2:
        st.subheader("Durée moyenne des promotions (jours)")
        st.bar_chart(
            df_category_display.set_index("PRODUCT_CATEGORY")[["AVG_PROMO_DURATION_DAYS"]]
        )

    if not df_category_display.empty:
        top_cat = df_category_display.iloc[0]
        st.info(
            f"🏆 **Top Catégorie** : {top_cat['PRODUCT_CATEGORY']} "
            f"({int(top_cat['NB_PROMOTIONS'])} promos, remise moy. "
            f"{top_cat['AVG_DISCOUNT_PCT']:.1f}%)"
        )

st.markdown("---")

# --------------------------------------------------
# Niveaux de remise
# --------------------------------------------------
if df_discount is not None and not df_discount.empty:
    st.header("💰 Distribution des Niveaux de Remise")

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Nombre de promotions")
        st.bar_chart(
            df_discount.set_index("DISCOUNT_RANGE")[["NB_PROMOTIONS"]]
        )
    
    with col2:
        st.subheader("Durée cumulée (jours)")
        st.bar_chart(
            df_discount.set_index("DISCOUNT_RANGE")[["TOTAL_DAYS"]]
        )

st.markdown("---")

# --------------------------------------------------
# Tables détaillées
# --------------------------------------------------
st.header("📋 Détails des Promotions")

tab1, tab2 = st.tabs(["Par Catégorie", "Par Région"])

with tab1:
    if df_category is not None and not df_category.empty:
        st.dataframe(
            df_category.sort_values("NB_PROMOTIONS", ascending=False),
            use_container_width=True,
            hide_index=True
        )

with tab2:
    if df_region is not None and not df_region.empty:
        st.dataframe(
            df_region.sort_values("NB_PROMOTIONS", ascending=False),
            use_container_width=True,
            hide_index=True
        )

st.markdown("---")

# --------------------------------------------------
# Recommandations dynamiques
# --------------------------------------------------
st.header("💡 Recommandations Stratégiques")

col1, col2 = st.columns(2)

with col1:
    st.success(
        "**✅ À faire**\n\n"
        "- Concentrer les promos sur catégories à forte rotation\n"
        "- Tester des remises **20–30%** (sweet spot)\n"
        "- Limiter la durée (< 15 jours) pour créer l'urgence\n"
        "- Cibler les régions avec forte réactivité"
    )

with col2:
    st.warning(
        "**⚠️ À éviter**\n\n"
        "- Sur-promotion (fatigue client)\n"
        "- Remises > 40% (cannibalise la marge)\n"
        "- Promotions trop longues (perte d'impact)\n"
        "- Négliger l'analyse post-promo"
    )

# Recommandations personnalisées selon les données
if df_category is not None and not df_category.empty:
    avg_discount = df_category["AVG_DISCOUNT_PCT"].mean()
    avg_duration = df_category["AVG_PROMO_DURATION_DAYS"].mean()
    
    if avg_discount > 35:
        st.error(f"🚨 **Alerte** : Remise moyenne de {avg_discount:.1f}% - Risque sur la marge !")
    
    if avg_duration > 20:
        st.warning(f"⏰ **Attention** : Durée moyenne de {avg_duration:.0f} jours - Risque de banalisation")

st.markdown("---")

# --------------------------------------------------
# Export des données
# --------------------------------------------------
st.header("📥 Export des Données")

col1, col2, col3 = st.columns(3)

with col1:
    if df_category is not None and not df_category.empty:
        if st.button("📊 Exporter Analyse Catégories"):
            csv = df_category.to_csv(index=False)
            st.download_button(
                label="Télécharger CSV",
                data=csv,
                file_name=f"promo_categories_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

with col2:
    if df_region is not None and not df_region.empty:
        if st.button("🌍 Exporter Analyse Régions"):
            csv = df_region.to_csv(index=False)
            st.download_button(
                label="Télécharger CSV",
                data=csv,
                file_name=f"promo_regions_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

with col3:
    if df_discount is not None and not df_discount.empty:
        if st.button("💰 Exporter Distribution Remises"):
            csv = df_discount.to_csv(index=False)
            st.download_button(
                label="Télécharger CSV",
                data=csv,
                file_name=f"promo_remises_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

st.markdown("---")
st.caption("🎯 Analyse Promotions | Berbere Lab Analytics")
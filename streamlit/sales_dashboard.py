# Dashboard de Ventes - Berbere Lab
# Streamlit natif Snowflake (AVEC filtres interactifs)

import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

# --------------------------------------------------
# Configuration
# --------------------------------------------------
st.set_page_config(
    page_title="Dashboard Ventes",
    page_icon="📊",
    layout="wide"
)

session = get_active_session()

st.title("📊 Dashboard des Ventes")
st.markdown("### Analyse détaillée des performances commerciales")
st.markdown("---")

# --------------------------------------------------
# Fonctions de chargement
# --------------------------------------------------
@st.cache_data
def get_distinct_regions():
    """Récupère la liste des régions disponibles"""
    return session.sql("""
        SELECT DISTINCT region
        FROM BERBERE_LAB.SILVER.financial_transactions_clean
        WHERE region IS NOT NULL
          AND transaction_type = 'Sale'
        ORDER BY region
    """).to_pandas()['REGION'].tolist()

@st.cache_data
def get_distinct_payment_methods():
    """Récupère les méthodes de paiement disponibles"""
    return session.sql("""
        SELECT DISTINCT payment_method
        FROM BERBERE_LAB.SILVER.financial_transactions_clean
        WHERE payment_method IS NOT NULL
          AND transaction_type = 'Sale'
        ORDER BY payment_method
    """).to_pandas()['PAYMENT_METHOD'].tolist()

@st.cache_data
def load_monthly_sales(regions, payment_methods, min_amount):
    """Charge les ventes mensuelles avec filtres"""
    if not regions or not payment_methods:
        return None
    
    regions_sql = ",".join([f"'{r}'" for r in regions])
    methods_sql = ",".join([f"'{m}'" for m in payment_methods])
    
    return session.sql(f"""
        SELECT
            DATE_TRUNC('month', transaction_date) AS month,
            region,
            COUNT(*) AS nb_transactions,
            SUM(amount) AS total_sales,
            AVG(amount) AS avg_transaction_value
        FROM BERBERE_LAB.SILVER.financial_transactions_clean
        WHERE transaction_type = 'Sale'
          AND region IN ({regions_sql})
          AND payment_method IN ({methods_sql})
          AND amount >= {min_amount}
        GROUP BY month, region
        ORDER BY month, region
    """).to_pandas()

@st.cache_data
def load_sales_by_region(regions, payment_methods, min_amount):
    """Charge les ventes par région avec filtres"""
    if not regions or not payment_methods:
        return None
    
    regions_sql = ",".join([f"'{r}'" for r in regions])
    methods_sql = ",".join([f"'{m}'" for m in payment_methods])
    
    return session.sql(f"""
        SELECT
            region,
            COUNT(*) AS nb_transactions,
            SUM(amount) AS total_sales,
            AVG(amount) AS avg_transaction_value
        FROM BERBERE_LAB.SILVER.financial_transactions_clean
        WHERE transaction_type = 'Sale'
          AND region IN ({regions_sql})
          AND payment_method IN ({methods_sql})
          AND amount >= {min_amount}
        GROUP BY region
        ORDER BY total_sales DESC
    """).to_pandas()

@st.cache_data
def load_sales_by_day(regions, payment_methods, min_amount):
    """Charge les ventes par jour de la semaine avec filtres"""
    if not regions or not payment_methods:
        return None
    
    regions_sql = ",".join([f"'{r}'" for r in regions])
    methods_sql = ",".join([f"'{m}'" for m in payment_methods])
    
    return session.sql(f"""
        SELECT
            DAYNAME(transaction_date) AS day_of_week,
            DAYOFWEEK(transaction_date) AS day_num,
            region,
            COUNT(*) AS nb_transactions,
            SUM(amount) AS total_sales,
            AVG(amount) AS avg_transaction_value
        FROM BERBERE_LAB.SILVER.financial_transactions_clean
        WHERE transaction_type = 'Sale'
          AND region IN ({regions_sql})
          AND payment_method IN ({methods_sql})
          AND amount >= {min_amount}
        GROUP BY day_of_week, day_num, region
        ORDER BY day_num, region
    """).to_pandas()

# --------------------------------------------------
# SIDEBAR - FILTRES
# --------------------------------------------------
st.sidebar.header("🎛️ Filtres")

# Charger les options de filtres
all_regions = get_distinct_regions()
all_payment_methods = get_distinct_payment_methods()

# Filtres interactifs
st.sidebar.subheader("📍 Géographie")
selected_regions = st.sidebar.multiselect(
    "Régions",
    options=all_regions,
    default=all_regions,
    help="Sélectionnez une ou plusieurs régions"
)

st.sidebar.subheader("💳 Paiement")
selected_payment_methods = st.sidebar.multiselect(
    "Méthodes de paiement",
    options=all_payment_methods,
    default=all_payment_methods,
    help="Filtrer par mode de paiement"
)

st.sidebar.subheader("💰 Montants")
min_transaction_amount = st.sidebar.slider(
    "Montant minimum de transaction (€)",
    min_value=0,
    max_value=1000,
    value=0,
    step=10,
    help="Exclure les petites transactions"
)

# Bouton pour réinitialiser les filtres
if st.sidebar.button("🔄 Réinitialiser les filtres"):
    st.rerun()

# --------------------------------------------------
# Chargement des données avec filtres
# --------------------------------------------------
df_month = load_monthly_sales(selected_regions, selected_payment_methods, min_transaction_amount)
df_region = load_sales_by_region(selected_regions, selected_payment_methods, min_transaction_amount)
df_day = load_sales_by_day(selected_regions, selected_payment_methods, min_transaction_amount)

# --------------------------------------------------
# Vérification des données
# --------------------------------------------------
if df_month is None or df_month.empty:
    st.warning("⚠️ Aucune donnée disponible avec ces filtres. Veuillez ajuster vos critères.")
    st.stop()

# --------------------------------------------------
# KPIs GLOBAUX
# --------------------------------------------------
# Agréger les données mensuelles pour KPIs globaux
total_sales = df_month["TOTAL_SALES"].sum()
total_tx = df_month["NB_TRANSACTIONS"].sum()
avg_ticket = total_sales / total_tx if total_tx > 0 else 0
best_month = df_month.groupby("MONTH")["TOTAL_SALES"].sum().idxmax()

col1, col2, col3, col4 = st.columns(4)
col1.metric("💰 Ventes Totales", f"€{total_sales:,.0f}")
col2.metric("🧾 Transactions", f"{int(total_tx):,}")
col3.metric("🛒 Panier Moyen", f"€{avg_ticket:.2f}")
col4.metric("🏆 Meilleur Mois", best_month.strftime("%B %Y"))

# Afficher les filtres actifs
st.info(f"📊 **Filtres actifs** : {len(selected_regions)} région(s), {len(selected_payment_methods)} mode(s) de paiement, montant min. €{min_transaction_amount}")

st.markdown("---")

# --------------------------------------------------
# VENTES MENSUELLES PAR RÉGION
# --------------------------------------------------
st.subheader("📈 Ventes Mensuelles par Région")

# Sélecteur de région pour le graphique mensuel
col1, col2 = st.columns([3, 1])
with col2:
    monthly_region_filter = st.selectbox(
        "Région à afficher",
        options=["Toutes"] + selected_regions,
        key="monthly_region"
    )

with col1:
    if monthly_region_filter == "Toutes":
        # Agréger toutes les régions
        df_month_agg = df_month.groupby("MONTH").agg({
            "TOTAL_SALES": "sum",
            "NB_TRANSACTIONS": "sum",
            "AVG_TRANSACTION_VALUE": "mean"
        }).reset_index()
        st.line_chart(df_month_agg.set_index("MONTH")[["TOTAL_SALES"]])
    else:
        # Filtrer par région sélectionnée
        df_month_filtered = df_month[df_month["REGION"] == monthly_region_filter]
        st.line_chart(df_month_filtered.set_index("MONTH")[["TOTAL_SALES"]])

# Tableau détaillé mensuel
with st.expander("📋 Voir le détail mensuel par région"):
    st.dataframe(
        df_month.sort_values(["MONTH", "TOTAL_SALES"], ascending=[True, False]),
        use_container_width=True,
        hide_index=True
    )

st.markdown("---")

# --------------------------------------------------
# VENTES PAR RÉGION
# --------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("🌍 Ventes par Région")
    st.bar_chart(df_region.set_index("REGION")[["TOTAL_SALES"]])

with col2:
    st.subheader("🧾 Transactions par Région")
    st.bar_chart(df_region.set_index("REGION")[["NB_TRANSACTIONS"]])

st.markdown("---")

# --------------------------------------------------
# PERFORMANCE PAR JOUR DE LA SEMAINE
# --------------------------------------------------
st.subheader("📅 Performance par Jour de la Semaine")

# Sélecteur de région pour le graphique jour de semaine
col1, col2 = st.columns([3, 1])
with col2:
    day_region_filter = st.selectbox(
        "Région à afficher",
        options=["Toutes"] + selected_regions,
        key="day_region"
    )

# Agréger ou filtrer selon sélection
if day_region_filter == "Toutes":
    df_day_display = df_day.groupby(["DAY_OF_WEEK", "DAY_NUM"]).agg({
        "TOTAL_SALES": "sum",
        "AVG_TRANSACTION_VALUE": "mean",
        "NB_TRANSACTIONS": "sum"
    }).reset_index().sort_values("DAY_NUM")
else:
    df_day_display = df_day[df_day["REGION"] == day_region_filter].sort_values("DAY_NUM")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Ventes totales**")
    st.bar_chart(df_day_display.set_index("DAY_OF_WEEK")[["TOTAL_SALES"]])

with col2:
    st.markdown("**Panier moyen**")
    st.line_chart(df_day_display.set_index("DAY_OF_WEEK")[["AVG_TRANSACTION_VALUE"]])

# Tableau détaillé par jour
with st.expander("📋 Voir le détail par jour de la semaine"):
    st.dataframe(
        df_day.sort_values(["DAY_NUM", "TOTAL_SALES"], ascending=[True, False]),
        use_container_width=True,
        hide_index=True
    )

st.markdown("---")

# --------------------------------------------------
# COMPARAISON RÉGIONS
# --------------------------------------------------
st.subheader("⚖️ Comparaison des Régions")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**Top 3 - Ventes**")
    top3_sales = df_region.nlargest(3, "TOTAL_SALES")[["REGION", "TOTAL_SALES"]]
    for idx, row in top3_sales.iterrows():
        st.metric(row["REGION"], f"€{row['TOTAL_SALES']:,.0f}")

with col2:
    st.markdown("**Top 3 - Transactions**")
    top3_tx = df_region.nlargest(3, "NB_TRANSACTIONS")[["REGION", "NB_TRANSACTIONS"]]
    for idx, row in top3_tx.iterrows():
        st.metric(row["REGION"], f"{int(row['NB_TRANSACTIONS']):,}")

with col3:
    st.markdown("**Top 3 - Panier Moyen**")
    top3_avg = df_region.nlargest(3, "AVG_TRANSACTION_VALUE")[["REGION", "AVG_TRANSACTION_VALUE"]]
    for idx, row in top3_avg.iterrows():
        st.metric(row["REGION"], f"€{row['AVG_TRANSACTION_VALUE']:.2f}")

st.markdown("---")

# --------------------------------------------------
# EXPORT DES DONNÉES
# --------------------------------------------------
st.subheader("📥 Export des Données")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📊 Exporter Ventes Mensuelles"):
        csv = df_month.to_csv(index=False)
        st.download_button(
            label="Télécharger CSV",
            data=csv,
            file_name=f"ventes_mensuelles_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

with col2:
    if st.button("🌍 Exporter Ventes par Région"):
        csv = df_region.to_csv(index=False)
        st.download_button(
            label="Télécharger CSV",
            data=csv,
            file_name=f"ventes_regions_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

with col3:
    if st.button("📅 Exporter Performance Hebdomadaire"):
        csv = df_day.to_csv(index=False)
        st.download_button(
            label="Télécharger CSV",
            data=csv,
            file_name=f"ventes_hebdo_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

st.markdown("---")
st.caption("📊 Berbere Lab Analytics | Streamlit in Snowflake")
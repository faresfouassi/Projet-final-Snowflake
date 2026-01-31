# ROI Marketing - Berbere Lab
# Streamlit natif Snowflake (SANS plotly)

import streamlit as st
from snowflake.snowpark.context import get_active_session

# --------------------------------------------------
# Configuration
# --------------------------------------------------
st.set_page_config(
    page_title="ROI Marketing",
    page_icon="📈",
    layout="wide"
)

session = get_active_session()

st.title("📈 ROI Marketing & Performance des Campagnes")
st.markdown("### Optimisation des investissements marketing")
st.markdown("---")

# --------------------------------------------------
# Chargement des données
# --------------------------------------------------
@st.cache_data
def load_campaign_performance():
    return session.sql("""
        WITH campaign_sales AS (
            SELECT
                mc.campaign_id,
                mc.campaign_name,
                mc.campaign_type,
                mc.region,
                mc.budget,
                mc.reach,
                mc.conversion_rate,
                COUNT(ft.transaction_id) AS nb_sales,
                SUM(ft.amount) AS total_sales
            FROM BERBERE_LAB.SILVER.marketing_campaigns_clean mc
            LEFT JOIN BERBERE_LAB.SILVER.financial_transactions_clean ft
                ON mc.region = ft.region
                AND ft.transaction_date BETWEEN mc.start_date AND mc.end_date
                AND ft.transaction_type = 'Sale'
            WHERE mc.budget > 0
            GROUP BY
                mc.campaign_id, mc.campaign_name, mc.campaign_type,
                mc.region, mc.budget, mc.reach, mc.conversion_rate
        )
        SELECT
            campaign_id,
            campaign_name,
            campaign_type,
            region,
            budget,
            reach,
            conversion_rate * 100 AS conversion_rate_pct,
            nb_sales,
            total_sales,
            total_sales / NULLIF(budget, 0) AS roi_ratio
        FROM campaign_sales
        ORDER BY roi_ratio DESC NULLS LAST
        LIMIT 50
    """).to_pandas()

@st.cache_data
def load_campaign_type_performance():
    return session.sql("""
        SELECT
            campaign_type,
            COUNT(*) AS nb_campaigns,
            SUM(budget) AS total_budget,
            AVG(budget) AS avg_budget,
            AVG(conversion_rate) * 100 AS avg_conversion_pct,
            AVG(reach) AS avg_reach
        FROM BERBERE_LAB.SILVER.marketing_campaigns_clean
        WHERE budget > 0
        GROUP BY campaign_type
        ORDER BY avg_conversion_pct DESC
    """).to_pandas()

@st.cache_data
def load_audience_performance():
    return session.sql("""
        SELECT
            target_audience,
            COUNT(*) AS nb_campaigns,
            AVG(conversion_rate) * 100 AS avg_conversion_pct,
            SUM(budget) AS total_budget
        FROM BERBERE_LAB.SILVER.marketing_campaigns_clean
        WHERE target_audience IS NOT NULL
        GROUP BY target_audience
        ORDER BY avg_conversion_pct DESC
        LIMIT 15
    """).to_pandas()

# --------------------------------------------------
# KPIs globaux
# --------------------------------------------------
df_campaigns = load_campaign_performance()

total_budget = df_campaigns["BUDGET"].sum()
total_sales = df_campaigns["TOTAL_SALES"].sum()
avg_roi = total_sales / total_budget if total_budget > 0 else 0
total_reach = df_campaigns["REACH"].sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("💰 Budget Total", f"€{total_budget:,.0f}")
col2.metric("📈 Ventes Générées", f"€{total_sales:,.0f}")
col3.metric("🔁 ROI Moyen", f"{avg_roi:.2f}x")
col4.metric("👥 Portée Totale", f"{total_reach:,.0f}")

# --------------------------------------------------
# Top campagnes
# --------------------------------------------------
st.markdown("---")
st.header("🏆 Top 10 Campagnes par ROI")

df_top = df_campaigns.head(10)

st.bar_chart(
    df_top.set_index("CAMPAIGN_NAME")[["ROI_RATIO"]]
)

st.subheader("📊 Top 3 – Détails")
for _, row in df_top.head(3).iterrows():
    col1, col2, col3 = st.columns(3)
    col1.metric("Campagne", row["CAMPAIGN_NAME"])
    col2.metric("ROI", f"{row['ROI_RATIO']:.2f}x")
    col3.metric("Conversion", f"{row['CONVERSION_RATE_PCT']:.1f}%")

# --------------------------------------------------
# Performance par type
# --------------------------------------------------
st.markdown("---")
st.header("📊 Performance par Type de Campagne")

df_type = load_campaign_type_performance()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Taux de Conversion Moyen (%)")
    st.bar_chart(
        df_type.set_index("CAMPAIGN_TYPE")[["AVG_CONVERSION_PCT"]]
    )

with col2:
    st.subheader("Budget Moyen (€)")
    st.bar_chart(
        df_type.set_index("CAMPAIGN_TYPE")[["AVG_BUDGET"]]
    )

best_type = df_type.iloc[0]
st.success(
    f"✅ **Type le plus performant** : {best_type['CAMPAIGN_TYPE']} "
    f"(Conv. moy. {best_type['AVG_CONVERSION_PCT']:.1f}%)"
)

# --------------------------------------------------
# Performance par audience
# --------------------------------------------------
st.markdown("---")
st.header("🎯 Performance par Public Cible")

df_audience = load_audience_performance()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Taux de Conversion Moyen (%)")
    st.bar_chart(
        df_audience.set_index("TARGET_AUDIENCE")[["AVG_CONVERSION_PCT"]]
    )

with col2:
    st.subheader("Budget Total (€)")
    st.bar_chart(
        df_audience.set_index("TARGET_AUDIENCE")[["TOTAL_BUDGET"]]
    )

# --------------------------------------------------
# Table & recommandations
# --------------------------------------------------
st.markdown("---")
st.header("📋 Détail des Campagnes")

st.dataframe(
    df_campaigns,
    use_container_width=True,
    hide_index=True
)

st.markdown("---")
st.header("💡 Recommandations Stratégiques")

col1, col2, col3 = st.columns(3)

with col1:
    st.success(
        "- Renforcer les campagnes à ROI élevé\n"
        "- Allouer plus de budget aux top performers\n"
        "- Répliquer les mécaniques gagnantes"
    )

with col2:
    st.info(
        "- Tester de nouveaux publics\n"
        "- A/B testing des messages\n"
        "- Optimisation continue des canaux"
    )

with col3:
    st.warning(
        "- Réduire ou stopper ROI < 1\n"
        "- Surveiller taux de conversion faibles\n"
        "- Éviter la saturation des audiences"
    )

# Alertes
low_roi = df_campaigns[df_campaigns["ROI_RATIO"] < 1]
if len(low_roi) > 0:
    st.error(
        f"🚨 {len(low_roi)} campagnes avec ROI < 1 "
        f"(Budget total €{low_roi['BUDGET'].sum():,.0f})"
    )

st.markdown("---")
st.caption("📈 ROI Marketing | Berbere Lab Analytics")

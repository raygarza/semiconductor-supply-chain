# =============================================================
# app.py
# Fragile by Design: Mapping the Global Semiconductor Supply Chain
# Author: Ray Garza | Austin TX | March 2026
# =============================================================

import streamlit as st
import pandas as pd
import plotly.express as px

# ── PAGE CONFIG ───────────────────────────────────────────────
st.set_page_config(
    page_title="Fragile by Design",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── HEADER ────────────────────────────────────────────────────
st.title("Fragile by Design")
st.subheader("Mapping the Global Semiconductor Supply Chain")
st.caption("Ray Garza | Austin, TX | March 2026")
st.divider()

# ── INTRO ─────────────────────────────────────────────────────
st.markdown("""
*[Intro copy goes here - written last when the story is complete.]*
""")

# Quick stat boxes
col1, col2, col3, col4 = st.columns(4)
col1.metric("Advanced Chips", "90%+", "made by TSMC")
col2.metric("Global Gallium Supply", "94%", "from China")
col3.metric("CHIPS Act Awarded", "$33B", "of $39B total")
col4.metric("Disruption Scenarios", "3 of 4", "already active")

st.divider()

# ── PANEL 1 - CAPEX CYCLE ─────────────────────────────────────
st.header("Panel 1 - The Capex Cycle")
st.markdown("""
*[2-3 sentences of context. What is the capex cycle and why does it matter?]*
""")

st.markdown("""
<iframe
    title="Capex Cycle Dashboard"
    width="100%"
    height="600"
    src="YOUR_POWER_BI_EMBED_URL_HERE"
    frameborder="0"
    allowFullScreen="true">
</iframe>
""", unsafe_allow_html=True)

st.caption("Source: Company IR filings via yfinance. Tickers: AMAT, ASML, LRCX, KLAC, UCTT, TSM, INTC, MU, NVDA.")
st.divider()

# ── PANEL 2 - THE CHOKEPOINTS ─────────────────────────────────
st.header("Panel 2 - The Chokepoints")
st.markdown("""
*[Context copy. Where do chips actually come from? Set up the map.]*
""")

st.markdown("""
<div class='tableauPlaceholder' id='viz_chokepoints'>
    <object class='tableauViz' width='100%' height='600'>
        <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F'/>
        <param name='embed_code_version' value='3'/>
        <param name='name' value='YOUR_TABLEAU_VIZ_NAME_HERE'/>
        <param name='tabs' value='no'/>
        <param name='toolbar' value='yes'/>
    </object>
</div>
""", unsafe_allow_html=True)

st.caption("Source: CEPII BACI International Trade Database HS17 V202601. HS codes 8541, 8542. 2018–2024.")
st.markdown("#### Top Semiconductor Exporting Countries (2024)")
st.info("Live data table loads here from Snowflake once pipeline is complete.")
st.divider()

# ── PANEL 3 - CHIPS ACT SCORECARD ────────────────────────────
st.header("Panel 3 - The CHIPS Act Scorecard")
st.markdown("""
*[What did the CHIPS Act promise? What's the gap between announced and reality?]*
""")

st.markdown("""
<iframe
    title="CHIPS Act Scorecard"
    width="100%"
    height="600"
    src="YOUR_POWER_BI_EMBED_URL_HERE"
    frameborder="0"
    allowFullScreen="true">
</iframe>
""", unsafe_allow_html=True)

try:
    chips_df = pd.read_csv("../raw_datasets/chips_act_awards_manual.csv")
    st.markdown("#### CHIPS Act Awards - Full Dataset")
    st.dataframe(
        chips_df[[
            "company", "state", "chips_type",
            "announced_amount_b", "finalized_amount_b",
            "status", "expected_completion"
        ]],
        use_container_width=True
    )
    st.caption("Source: Semiconductor Industry Association tracker + GAO Report GAO-26-107882. Compiled manually March 2026.")
except:
    st.warning("CHIPS Act dataset not found - check file path.")

st.divider()

# ── PANEL 4 - BLOCKADE SCENARIO ───────────────────────────────
st.header("Panel 4 - The Blockade Scenario")
st.markdown("""
*[Your longest copy section - 2-3 paragraphs. Explain the four scenarios.
Note that three are already active. This is the McKinsey moment.]*
""")

st.markdown("""
<div class='tableauPlaceholder' id='viz_blockade'>
    <object class='tableauViz' width='100%' height='600'>
        <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F'/>
        <param name='embed_code_version' value='3'/>
        <param name='name' value='YOUR_TABLEAU_VIZ_NAME_HERE'/>
        <param name='tabs' value='no'/>
        <param name='toolbar' value='yes'/>
    </object>
</div>
""", unsafe_allow_html=True)

try:
    blockade_df = pd.read_csv("../raw_datasets/blockade_scenario_model.csv")
    st.markdown("#### Full Scenario Model - All Sources Visible")
    st.dataframe(
        blockade_df[[
            "scenario_code", "industry",
            "tsmc_dependency_pct", "typical_inventory_days",
            "hits_wall_60day", "hits_wall_90day",
            "us_domestic_coverage_pct_2030",
            "scenario_already_active"
        ]],
        use_container_width=True
    )
    st.caption("Source: Derived model. Full methodology in source_registry.csv and data_dictionary.csv in GitHub repo.")
except:
    st.warning("Blockade scenario dataset not found - check file path.")

st.divider()

# ── THE STACK ─────────────────────────────────────────────────
st.header("The Stack")
st.markdown("""
**Databricks** (Spark ETL) → **Snowflake** (star schema) →
**Tableau Public** + **Power BI** (visualization) →
**Streamlit** (this app) → **Docker** (containerized)

Full data lineage documentation - source registry and data dictionary -
available in the [GitHub repo](https://github.com/raygarza/semiconductor-supply-chain).

*If I can't source it, it's not in the project.*
""")

st.divider()

# ── FOOTER ────────────────────────────────────────────────────
st.markdown("""
**Ray Garza** | Austin, TX | raydev@protonmail.com

[GitHub](https://github.com/raygarza/semiconductor-supply-chain) ·
[LinkedIn](#) ·


*Currently open to data analyst, BI analyst, and analytics engineer roles in Austin TX and remote.*
""")
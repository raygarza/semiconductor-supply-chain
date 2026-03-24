# Fragile by Design: Mapping the Global Semiconductor Supply Chain

**Built by Ray Garza | Austin, TX | March 2026**

[![Streamlit App](https://img.shields.io/badge/Live_App-Streamlit-red)](https://your-streamlit-url-here)
[![Tableau Public](https://img.shields.io/badge/Dashboards-Tableau_Public-blue)](https://public.tableau.com/your-profile)
[![LinkedIn](https://img.shields.io/badge/Follow_Along-LinkedIn-0077B5)](https://linkedin.com/in/your-profile)

---

## What This Is

Everyone knows Taiwan makes chips. Fewer people know how concentrated that actually is - or that three of the four scenarios that could disrupt the global supply chain are already partially underway.

This project maps global semiconductor trade flows, equipment manufacturer capex cycles, CHIPS Act investment reality vs. hype, and models four disruption scenarios - from full military blockade to the earthquake nobody's talking about - with every number sourced and documented.

The question this answers: **Is the chip supply chain actually getting less fragile, or are we just moving the assembly line while the chokepoints stay the same?**

Spoiler: the data is more interesting than the headlines.

---

## The Dashboard

Four panels. Scroll top to bottom in about four minutes.

| Panel | What It Shows |
|---|---|
| **1 - The Capex Cycle** | Equipment company revenue vs. fab spending over 10 years. Where are we in the cycle right now? |
| **2 - The Chokepoints** | Global semiconductor trade flows by country. Taiwan is very red. |
| **3 - The CHIPS Act Scorecard** | Announced vs. finalized vs. renegotiating. The gap between those numbers is the story. |
| **4 - The Blockade Scenario** | Four disruption scenarios × seven industries × four time horizons. Three of four scenarios are already partially active. |

---

## The Stack

This project follows a **medallion architecture** - a standard pattern used by professional data engineering teams.

```
BRONZE (Raw)          SILVER (Clean)         GOLD (Modeled)         SERVE
─────────────         ──────────────         ──────────────         ─────
BACI trade CSVs  →    Spark/Databricks   →   Snowflake          →   Tableau Public
yfinance pull         cleaning notebook      star schema            + Streamlit app
CHIPS Act CSV         Parquet output         SQL views              + Docker container
Blockade model
```

### Tools Used

| Tool | Purpose | Why This Tool |
|---|---|---|
| **Databricks Community Edition** | Spark ETL processing | Industry standard for distributed data processing. Justifiable at this data scale - BACI trade data is multi-GB across 200 countries. |
| **Apache Spark** | Distributed data transformation | Parallel processing of international trade records. Bronze → Silver layer transformation. |
| **Snowflake** (free trial) | Cloud data warehouse | Star schema modeling. Gold layer. Tableau connects here, not to raw files. |
| **Tableau Public** | Visualization | Free, embeddable, professional-grade. Three published dashboards. |
| **Streamlit** | Web app deployment | Python-native, fast to build, public URL. The front door. |
| **Docker** | Containerization | Reproducible local environment. One command to run the app anywhere. |
| **Python / Pandas** | Data wrangling, yfinance pull | Core data language. Used in Databricks notebooks and Streamlit app. |
| **Git / GitHub** | Version control | Full commit history. Every dataset change tracked. |

---

## Data Sources

All datasets are documented in [`/docs/source_registry.csv`](docs/source_registry.csv) with publisher, URL, access date, and reliability tier.

Every column in every dataset is defined in [`/docs/data_dictionary.csv`](docs/data_dictionary.csv) including transformation notes and verification status.

### Reliability Tiers

| Tier | Label | Means |
|---|---|---|
| 1 | PRIMARY | Direct from issuing organization - SEC filings, government reports, official trackers |
| 2 | SECONDARY | Reputable analysis citing primary sources - think tanks, industry research firms |
| 3 | DERIVED | Calculated or modeled from Tier 1/2 sources - methodology documented |
| 4 | ESTIMATED | Industry consensus - clearly flagged in viz and documentation |

### Quick Source Summary

| Dataset | Source | Tier |
|---|---|---|
| Global trade flows | CEPII BACI (UN Comtrade) | 1 - PRIMARY |
| Company financials | yfinance / SEC filings | 1 - PRIMARY |
| CHIPS Act awards | SIA Tracker + GAO Report GAO-26-107882 | 1 - PRIMARY |
| China export controls | USITC + AP + Stimson Center | 1-2 |
| Earthquake risk | Moody's seismic analysis + USGS + TrendForce | 2 - SECONDARY |
| Blockade scenario model | Derived from above + DoE + PIIE + ISW/AEI | 3 - DERIVED |

---

## Repo Structure

```
/
├── README.md                          ← You are here
│
├── /databricks_notebooks/
│   ├── 01_bronze_ingest.ipynb         ← Raw data landing, schema inspection
│   ├── 02_silver_transform.ipynb      ← Spark cleaning, Parquet output  ← THE ARTIFACT
│   └── 03_silver_validate.ipynb       ← Row count checks, quality gates
│
├── /sql/
│   ├── schema_create.sql              ← Snowflake star schema DDL
│   ├── dim_tables.sql                 ← Dimension table loads
│   └── gold_views.sql                 ← Views that Tableau queries
│
├── /streamlit_app/
│   ├── app.py                         ← Main Streamlit application
│   ├── requirements.txt               ← Python dependencies
│   └── Dockerfile                     ← Container definition
│
├── /data/
│   ├── chips_act_awards_manual.csv    ← CHIPS Act awards (manually compiled)
│   ├── blockade_scenario_model.csv    ← Disruption scenario model
│   └── /raw/                          ← Raw downloaded files (gitignored if large)
│
└── /docs/
    ├── source_registry.csv            ← All 25+ sources with URLs and tiers
    └── data_dictionary.csv            ← Every column defined and sourced
```

---

## The Blockade Scenario - Why Four Scenarios

Most supply chain analyses model one scenario: China invades Taiwan. That's the one everyone talks about. Here's what the data actually shows:

| Scenario | Status as of March 2026 |
|---|---|
| **A - Full Military Blockade** | Hypothetical. PLA has practiced it explicitly (Dec 2025 drills). |
| **B - Gray Zone / Partial Blockade** | **Already active.** Coast guard encirclement, port blockade drills ongoing. |
| **C - Critical Minerals Retaliation** | **Already active.** China banned gallium/germanium exports to US Dec 3, 2024. |
| **D - Earthquake Near Hsinchu** | **Partially active.** Three major quakes since 2024. Hsinchu fabs hit in Dec 2025. |

Three of four scenarios are not hypothetical. They're present tense.

---

## A Note on the Numbers

Every figure in this project has a source. Numbers I modeled or derived are labeled as such in the data dictionary and flagged in the visualizations. If you see a number and want to know where it came from, open `source_registry.csv`, find the `source_id` referenced in `data_dictionary.csv`, and the full citation is there.

If I can't source it, it's not in the project.

---

## About This Project

I work in the semiconductor industry in Austin, TX - currently embedded in the supply chain layer that sits between equipment makers and the fabs themselves. That's about as deep in the stack as you can get without actually running a fab.

I built this to understand the industry I work in. I also built it to demonstrate a full modern data stack to hiring managers who want to see more than a Power BI dashboard connected to a CSV.

The tools are real. The data is sourced. The story is current.

*Full data lineage documentation available in `/docs/`. Databricks notebooks published as ETL artifacts. Questions, corrections, and job offers welcome.*

---

## Running Locally

```bash
# Clone the repo
git clone https://github.com/yourusername/semiconductor-supply-chain
cd semiconductor-supply-chain

# Run with Docker (recommended)
docker build -t semicon-app ./streamlit_app
docker run -p 8501:8501 semicon-app

# Or run directly
pip install -r streamlit_app/requirements.txt
streamlit run streamlit_app/app.py
```

---

## Contact

**Ray Garza**
Austin, TX
raydev@protonmail.com
[LinkedIn](https://linkedin.com/in/your-profile) | [Portfolio](https://your-portfolio-url)

*Currently open to data analyst, BI analyst, and analytics engineer roles in Austin TX and remote.*

---

*Data current as of March 2026. Geopolitical status fields will require updates as situation evolves. See `data_dictionary.csv` for fields marked `CURRENT_AS_OF_2026_03_24`.*

# Strategic Merger in the OTT Domain: Data-Driven Insights for Hotstar-JioCinema Integration

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Pandas](https://img.shields.io/badge/Pandas-2.1-green?logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Plotly](https://img.shields.io/badge/Plotly-5.17-orange?logo=plotly&logoColor=white)](https://plotly.com/python/)
[![Scikit-learn](https://img.shields.io/badge/Scikit-learn-1.3-purple?logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-gray?logo=jupyter&logoColor=white)](https://jupyter.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Project Overview

This project provides a comprehensive, executable data analysis pipeline to evaluate the strategic merger of **Disney+ Hotstar** (referred to as "Jotstar" in datasets) and **JioCinema** (referred to as "Liocinema") in India's competitive Over-The-Top (OTT) streaming market. By analyzing anonymized subscriber demographics, content consumption patterns, and metadata from two MySQL databases (`jotstar_db` and `liocinema_db`), the pipeline uncovers actionable insights into user acquisition, retention dynamics, and behavioral segmentation.

**Why This Project?**  
India's OTT sector is exploding, projected to reach $5 billion in revenue by 2027 (Statista, 2025). A Hotstar-JioCinema merger could create a dominant player with 100M+ users, but success hinges on synergies like cross-platform content bundling and targeted retention strategies. This analysis simulates these opportunities, focusing on:  
- **Post-Pilot Growth**: Identifying city tiers (Tier 1-3) with the fastest digital subscriber expansion.  
- **Retention Cohorts**: Benchmarking 30/60/90-day retention by age group.  
- **User Segmentation**: Clustering users by watch time and device to prioritize high-value segments (e.g., "Heavy Multi-Device Users").  

The core deliverable is `Analysis.ipynb`, a Jupyter Notebook that runs end-to-end in under 5 minutes on standard hardware. It generates interactive Plotly visualizations, processed CSVs for further analysis, a business-ready report, and metadata summaries. Dual data loading (MySQL or CSV) ensures flexibility for development or production environments.

**Key Outcomes**:  
- JioCinema shows 27.12% peak MoM growth in Tier 3 cities vs. Hotstar's 1.57%.  
- Hotstar leads 90-day retention (5.13% vs. 2.14%), strongest in 35-44 age group.  
- 5 user clusters identified, with "Heavy Multi-Device Users" driving 20% of watch time.  

**Target Audience**: Data analysts, product managers, and executives in media/tech mergers. Fork and adapt for similar OTT benchmarks.

## Research Questions & Analysis Scope

The notebook addresses two primary questions through modular sections:

1. **Q1: Monthly Acquisition Cohorts & Retention**  
   - *Question*: What are the Month-over-Month (MoM) subscriber growth trends and 30/60/90-day retention rates for Hotstar vs. JioCinema, segmented by age group? (Ties to city-tier growth via `city_tier` grouping.)  
   - *Approach*: Pandas `Grouper` for cohorts, pivot tables for retention curves, Plotly line/bar plots.  
   - *Output*: Growth curves, retention heatmaps, summary table (e.g., 35-44 cohort at 6.5% 90-day retention for Hotstar).

2. **Q2: Watch Time Segmentation**  
   - *Question*: Cluster users on `total_watch_time_mins` and `device_type` into 3-5 segments; what % of total watch time do they represent per platform?  
   - *Approach*: Scikit-learn KMeans (validated by silhouette score ~0.42), StandardScaler, Plotly barplots.  
   - *Output*: 5 segments (e.g., "Heavy Multi-Device Users": 20% watch time), profile CSV.

These build on exploratory data analysis (EDA) to simulate merger impacts, like +15% retention uplift via age-targeted bundling.

## Methodology in Brief

- **Data Flow**: Load → Preprocess (null fills, datetime parsing) → Analyze (growth calcs, clustering) → Visualize & Report.  
- **Tools**: Pandas for ETL, Statsmodels for stats, Scikit-learn for ML, Plotly for interactivity.  
- **Robustness**: Logging, error handling, warnings suppression; validates ~220K rows (37K Hotstar subs, 183K JioCinema).  
- **Assumptions**: Data snapshot to Oct 2025; anonymized users; prorated revenue not modeled (extendable).  

For full reproducibility, see [Notebook Sections](#notebook-organization).

## Project Structure

```
Strategic Merger in the OTT Domain/
├── notebooks/                          # Core analyses
│   └── Analysis.ipynb                  # Main executable: Imports → Insights
├── data/                               # Datasets (CSVs or DB configs)
│   ├── raw/                            # Input CSVs (e.g., subscribers_hotstar.csv)
│   │   ├── content_consumption_hotstar.csv
│   │   ├── contents_hotstar.csv
│   │   ├── subscribers_hotstar.csv
│   │   ├── content_consumption_jiocinema.csv
│   │   ├── contents_jiocinema.csv
│   │   └── subscribers_jiocinema.csv
│   └── processed/                      # Auto-generated CSVs (e.g., retention_cohorts.csv)
├── outputs/                            # Reports & artifacts
│   ├── business_report.md              # Auto-gen insights narrative
│   ├── metadata_summary.csv            # Data quality/stats
│   ├── analysis_csvs/                  # Processed outputs (e.g., mom_growth.csv)
│   │   ├── retention_summary.csv
│   │   └── clusters_segments.csv
│   └── figures/                        # Plots (HTML/PNG)
│       ├── mom_growth.html
│       └── retention_by_age.png
├── README.md                           # This guide
├── requirements.txt                    # Dependencies
└── LICENSE                             # MIT                             # MIT License
```

## Notebook Organization

`Analysis.ipynb` (77 pages in Colab PDF export) is structured for sequential execution:

1. **Imports & Configuration** (Pages 1-2): Load libraries (Pandas, Plotly, Scikit-learn), set logging/styles, define retention periods ([30,60,90] days).  
2. **Loading the Database & Giving Proper Names** (Pages 2-3): Dual-mode ingestion; assign tables (e.g., `subscribers_hotstar = pd.read_csv(...)`).  
3. **Preprocessing the Databases** (Pages 3-7): Per-table cleaning—null fills ('Inactive'/'None'), datetime coercion, strip spaces. Subsections for each table/platform.  
4. **Exploratory Data Analysis** (Pages 7-8): Validation/prep for growth/retention.  
5. **Q1: Monthly Acquisition Cohorts** (Pages 8-15): MoM calc (`pct_change()`), Plotly lineplot; retention by active days, age-group bars.  
6. **Insights Generator** (Pages 15-17): Auto-summary (e.g., "JioCinema higher growth potential"), recs (e.g., "Focus 35-44 marketing").  
7. **Q2: Watch Time Segmentation** (Pages 17-25+): Merge data, scale features, KMeans (n=5 via silhouette), segment conditions/labels, bar viz.  

Run "Restart & Run All" to populate `outputs/`. Logs confirm steps (e.g., "✅ Retention analysis completed").

## Technologies & Dependencies

| Category       | Key Libraries                  | Role                              |
|----------------|--------------------------------|-----------------------------------|
| Data           | Pandas, NumPy, SQLAlchemy     | ETL, querying, aggregation        |
| Visualization  | Plotly, Matplotlib, Seaborn   | Interactive charts, heatmaps      |
| ML/Stats       | Scikit-learn, Statsmodels     | Clustering, cohort analysis       |
| Database       | PyMySQL                       | MySQL connections                 |
| Utilities      | Logging, Warnings, IPython    | Debugging, displays, config       |

Install via `pip install -r requirements.txt` (pinned versions for reproducibility).

## Data Loading Options

Configure `LOAD_MODE` in Section 2 of the notebook.

### Option 1: Direct Database Access (Live MySQL)
- **Ideal For**: Real-time data pulls.  
- **Setup**: Update `password = quote_plus('your_password')`; assumes localhost:3306.  
- **Execution**:  
  ```python
  db_names = ['jotstar_db', 'liocinema_db']
  all_data = {}  # Fills via create_engine, SHOW TABLES, pd.read_sql
  ```  
- **Output**: Dict like `{'jotstar_db.subscribers': df}`.

### Option 2: CSV Imports (Offline/Portable)
- **Ideal For**: Local dev, no DB access.  
- **Setup**: Export CSVs to `data/raw/` (e.g., via MySQL Workbench).  
- **Execution**:  
  ```python
  subscribers_hotstar = pd.read_csv('data/raw/subscribers_hotstar.csv')
  # Assign to all_data dict similarly
  ```  
- **Automation Tip**: Add a script to dump DB → CSV if needed.

Validation runs post-load (e.g., null sums, row counts).

## Setup & Reproduction

1. **Clone Repository**:  
   ```bash
   git clone https://github.com/prathamesh095/Data-Analysis.git
   cd "Data-Analysis/Strategic Merger in the OTT Domain"
   ```

2. **Install Dependencies**:  
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac; or venv\Scripts\activate (Windows)
   pip install -r requirements.txt
   ```

3. **Prepare Data**: Select Option 1 or 2; ensure CSVs in `data/raw/` if using CSV mode.

4. **Run the Analysis**:  
   ```bash
   jupyter notebook  # Open Analysis.ipynb
   # In notebook: Kernel > Restart & Run All
   ```  
   - Batch mode: `jupyter nbconvert --to notebook --execute --inplace notebooks/Analysis.ipynb`.  
   - Outputs auto-save to `outputs/`.

**Hardware**: 8GB RAM sufficient; ~300K rows processed in <2min. Set `os.environ['LOKY_MAX_CPU_COUNT'] = '8'` for parallelism.

**Troubleshooting**:  
- DB errors: Check creds/port.  
- Import issues: Verify Python 3.12.  
- OOM: Sample data (e.g., `df.sample(0.1)`).

## Usage Examples

- **End-to-End Run**: Execute full notebook → Get `business_report.md` with KPIs/recs.  
- **Focus on Q1**: Run to Section 5 → Export `mom_growth.csv` for Excel.  
- **Customize Segmentation**: Change `n_clusters=4` in Q2; re-run for new silhouette.  
- **Extend Insights**: Uncomment ARIMA in utils for 2026 forecasts.  
- **Share Outputs**: Open `mom_growth.html` in browser for interactive zoom.

## Business Report & Metadata

Running the notebook generates:  
- **business_report.md** (`outputs/`): Markdown executive summary—e.g., "Max MoM Growth: JioCinema 27.12% (Tier 3 driver)"; recs like "Invest in Hotstar for 90-day retention boost."  
- **metadata_summary.csv** (`outputs/`): Table of data stats (e.g., Hotstar subs: 37,530 rows, 0% nulls in `user_id`).  
- **analysis_csvs/** (`outputs/`): Ready-for-dashboard files like `retention_summary.csv` (columns: Platform, Age_Group, Retention_30_days_%, etc.) and `clusters_segments.csv` (User_ID, Cluster_Label, Watch_Time_Share_%).

Use these for PowerBI/Tableau integrations or stakeholder decks.

## Data Governance & Limitations

- **Sources**: Anonymized extracts compliant with India's DPDP Act 2023. No PII beyond hashed IDs.  
- **Schema Overview**:  
  | Table                  | Key Columns                          | Rows (Hotstar/JioCinema) |
  |------------------------|--------------------------------------|---------------------------|
  | Subscribers           | user_id, age_group, city_tier, subscription_date, plan | 37K / 183K               |
  | Content Consumption   | user_id, device_type, total_watch_time_mins | 134K / 183K              |
  | Contents              | content_id, genre, language, run_time | 1.2K / 1.2K              |  
- **Limitations**: Static snapshot (up to Oct 2025); city-tier proxy for geo (no lat/long). Potential age bias—stratify in extensions. Scales to 1M+ rows with Dask.

## Contributing

Contributions welcome!  
- **How**: Fork > Branch (`feat/new-metric`) > PR with tests (notebook assertions).  
- **Ideas**: Add churn prediction (lifelines), causal uplift (DoWhy), or Streamlit dashboard.  
- **Guidelines**: PEP 8; docstrings; <80-char lines. See CONTRIBUTING.md (forthcoming).

## Future Work

- Integrate real-time APIs (e.g., Polygon for revenue proxies).  
- Causal modeling for merger simulations.  
- Scalable ETL with Airflow.  
- Sentiment analysis from X posts on OTT mergers.

## References

- Statista. (2025). *Video on Demand - India*.  
- EY-FICCI. (2025). *Media & Entertainment Report*.  
- Aurélien Géron. *Hands-On Machine Learning with Scikit-Learn* (3rd Ed.).

## License & Attribution

MIT License—use freely, attribute as "OTT Merger Analytics: Hotstar-JioCinema (Jadhav, 2025)". Include GitHub link.

## Contact

**Prathamesh Pawar**  
- Email: [pawarprathamesh095@gmail.com](mailto:pawarprathamesh095@gmail.com)  
- LinkedIn: [linkedin.com/in/prathamesh095](https://www.linkedin.com/in/prathamesh095)  
- GitHub: [github.com/prathamesh095](https://github.com/prathamesh095)  

Open issues for feedback!

---

*Last Updated: October 18, 2025 | – Enhanced explanations & structure.*  
*Empowering data-driven OTT decisions. 🚀*

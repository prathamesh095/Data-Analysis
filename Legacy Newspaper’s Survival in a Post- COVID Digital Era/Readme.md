# Legacy Newspaper’s Survival in a Post-COVID Digital Era

[![Python](https://img.shields.io/badge/Python-3.12.3-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Jupyter Notebook](https://img.shields.io/badge/Jupyter-Notebook-orange.svg)](https://jupyter.org/)

## 📖 Project Overview

This repository presents a comprehensive data analytics project exploring the challenges and opportunities for legacy newspapers in transitioning to a digital-first model post-COVID-19. Using real-world-inspired datasets on print circulation, ad revenue, city digital readiness, and digital pilot programs, the analysis uncovers trends, correlations, and actionable insights to guide strategic decisions.

The project simulates a newspaper company's operations across Indian cities, evaluating print decline, digital adoption, and revenue optimization. Key themes include:
- **Declining print trends** amid rising digital penetration.
- **Ad revenue dynamics** across categories and currencies.
- **City-level disparities** in readiness and engagement.
- **ROI metrics** for digital vs. traditional channels.

Built with Python and Jupyter Notebook, this project is ideal for data enthusiasts, business analysts, or media strategists interested in media transformation.

**Repo Link:** [Legacy Newspaper’s Survival in a Post-COVID Digital Era](https://github.com/yourusername/legacy-newspaper-survival) *(Replace with actual GitHub URL)*

## 🎯 Project Objectives

The core goal is to answer 20+ strategic questions, such as:
- How has print circulation trended YoY, and which cities lead in net sales?
- What ad categories show resilience, and how does revenue correlate with circulation?
- Which cities have high digital readiness but low pilot engagement (untapped potential)?
- What's the transition index from print to digital users, and its YoY evolution?

These insights help identify growth levers, like targeting Tier 3 cities with Hindi editions or optimizing festive ad campaigns.

## 📊 Dataset Overview

The project uses a star schema with dimensional and fact tables stored in a MySQL database (`legacy_newspaper`). Key datasets include:

| Table Name              | Description                          | Key Columns                          | Rows (Approx.) |
|-------------------------|--------------------------------------|--------------------------------------|----------------|
| `dim_city`             | City demographics and tiers          | `city_id`, `city`, `state`, `tier`   | 500+          |
| `dim_ad_category`      | Ad categories and groups             | `ad_category_id`, `standard_ad_category`, `category_group` | 4             |
| `fact_print_sales`     | Monthly print sales and returns      | `edition_ID`, `Copies Sold`, `Net_Circulation`, `date` | 36,000+       |
| `fact_ad_revenue`      | Quarterly ad revenue by category     | `edition_id`, `ad_category`, `ad_revenue`, `quarter`, `currency` | 20,000+       |
| `fact_city_readiness`  | Quarterly city metrics (literacy, penetration) | `city_id`, `literacy_rate`, `smartphone_penetration`, `quarter` | 24,000+       |
| `fact_digital_pilot`   | Digital pilot metrics (costs, engagement) | `city_id`, `platform`, `users_reached`, `avg_bounce_rate` | 10,000+       |

**Data Generation:** The notebook includes scripts to generate 500+ synthetic cities and 10k+ entries for scalability. Real data can be substituted via CSV imports.

## 🔧 Technologies & Tools

- **Languages:** Python 3.12.3
- **Libraries:**
  - Data Manipulation: `pandas`, `numpy`
  - Visualization: `matplotlib`, `seaborn`, `plotly`
  - Database: `SQLAlchemy`, `pymysql`
  - Scaling: `sklearn` (MinMaxScaler)
- **Environment:** Jupyter Notebook
- **Database:** MySQL (local setup recommended)

## 🚀 Getting Started

### Prerequisites
- Python 3.12+
- MySQL Server (e.g., via XAMPP or Docker)
- Jupyter Lab/Notebook

### Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/legacy-newspaper-survival.git
   cd legacy-newspaper-survival
   ```

2. **Set Up Virtual Environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Database Setup:**
   - Create a MySQL database named `legacy_newspaper`.
   - Update the connection string in `main.ipynb` (Cell under "Connecting to the DataBase"):
     ```python
     password = quote_plus("your_mysql_password")  # Replace with your password
     ```
   - Run the data generation script in `main.ipynb` (under "Data Generation") to populate tables:
     ```python
     # Execute the provided code to add 500 new cities and update fact tables
     ```

4. **Launch Jupyter:**
   ```bash
   jupyter notebook
   ```
   Open `main.ipynb` and run all cells sequentially.

### Requirements File (`requirements.txt`)
```
pandas==2.1.4
numpy==1.25.2
matplotlib==3.8.2
seaborn==0.13.0
plotly==5.17.0
sqlalchemy==2.0.23
pymysql==1.1.0
scikit-learn==1.3.2
```

## 📈 Key Analyses & Visualizations

The notebook is structured into sections with interactive Plotly charts and tables. Highlights:

1. **Print Circulation Trends**: YoY decline in copies printed/sold (line chart).
2. **Top Performing Cities**: Ranked by net circulation (bar chart).
3. **Print Waste Analysis**: Cities with highest waste gaps (gradient bar chart).
4. **Ad Revenue by Category**: Evolution across FMCG, Real Estate, etc. (multi-line chart).
5. **City-Level Ad Performance**: Correlation scatter plot (r ≈ 0.75).
6. **Digital Readiness vs. Engagement**: Target cities for pilots (dual-line chart).
7. **Ad Revenue ROI**: Per-copy revenue trends (bar + YoY heatmap).
8. **Language Impact**: Hindi vs. English in Tier 1/3 (distribution bar).
9. **Internet Penetration Correlation**: With FMCG ad revenue (r ≈ 0.62).
10. **Literacy Growth Alignment**: With digital success (bar chart).
11. **Bounce Rates by Platform**: WhatsApp vs. App averages (table).
12. **Marketing Cost Impact**: On users reached (scatter with trendline).
13. **Dev Cost Trends in Tier 3**: Vs. downloads (correlation plot).
14. **Ad Comments & Spikes**: Festive pushes (line chart).
15. **High-Revenue Brands**: Category frequency (bar chart).
16. **YoY Circulation by State/Language**: Growth table.
17. **Currency-Standardized Revenue**: Box plot by quarter.
18. **Literacy vs. Bounce Rates**: Violin plot (negative correlation).
19. **Penetration Gaps**: Scatter with thresholds.
20. **Transition Index**: Digital/print ratio evolution (subplot grid).

All visualizations are interactive—hover for details!

## 📊 Sample Insights

| Insight | Key Finding | Implication |
|---------|-------------|-------------|
| Print Trends | 15-20% YoY decline in net circulation (2019-2024) | Accelerate digital shift. |
| Top Cities | Delhi & Mumbai lead with 5M+ annual circulation. | Prioritize premium ads here. |
| Ad Categories | FMCG stable (+2% YoY); Real Estate down 18%. | Diversify to Government ads. |
| Digital Gaps | 25+ Tier 2 cities: High readiness (0.75+ score), low engagement (0.25-). | Targeted marketing pilots. |
| Transition Index | Avg. 12% digital/print ratio; +5% YoY growth in Tier 1. | Hindi editions boost in Tier 3. |

## 🤝 Contributing

Contributions welcome! Fork the repo, create a feature branch (`git checkout -b feature/amazing-new-chart`), and submit a PR. Focus on:
- New analyses (e.g., ML predictions for revenue).
- Data enhancements (real-world integrations).
- Bug fixes in preprocessing.

Please adhere to PEP 8 style and add tests.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

- **Name:** Prathamesh sanjay Pawar
- **GitHub:** [https://github.com/prathamesh095 ](https://github.com/prathamesh095)
- **LinkedIn:** [https://www.linkedin.com/in/prathamesh095](https://www.linkedin.com/in/prathamesh095)
- **Email:** [pawarprathamesh095@gmail.com](pawarprathamesh095@gmail.com)


Feel free to connect for collaborations on data analytics, media insights, or open-source projects!

## 🙏 Acknowledgments

- Inspired by media industry challenges post-COVID.
- Dummy data generation for educational scalability.
- Thanks to Plotly for stunning interactive viz!

---

*Last Updated: October 06, 2025*  
*Questions? Open an issue or reach out!*

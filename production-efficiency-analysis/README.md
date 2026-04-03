# 🚀 Production Line Efficiency Analysis

### Data-Driven Optimization of Manufacturing Performance (OEE, Downtime, Quality)

---

## 📌 Project Overview

This project focuses on analyzing and improving production efficiency in a manufacturing environment using **data-driven diagnostic analytics**.

A mid-scale automotive component plant was experiencing:

* Low production efficiency
* High machine idle time
* Inconsistent cycle times
* Increasing rejection rates

Despite having modern CNC machines, the plant was unable to achieve its target output.

👉 This project identifies **why inefficiencies occur** and provides **actionable business recommendations** — without relying on predictive models.

---

## 🎯 Objectives

* Improve **Overall Equipment Efficiency (OEE)**
* Reduce **downtime and idle losses**
* Stabilize **cycle time variability**
* Reduce **rejection rates**
* Identify **process bottlenecks**

---

## 🧠 Project Type

✔ Diagnostic Analytics
✔ Industrial Data Analysis
✔ Business-Focused Insights

❌ No Machine Learning
❌ No Predictive Modeling
❌ No Over-Engineering

---

## 🏭 Business Context

The analysis is based on real-world manufacturing data sources such as:

* Machine logs (PLC / sensor data)
* Production records (shift-wise output)
* Quality inspection data
* Maintenance logs

---

## 🧱 Project Structure

```
production-efficiency-analysis/

├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
│   ├── 01_data_cleaning.ipynb
│   ├── 02_kpi_analysis.ipynb
│   ├── 03_downtime_analysis.ipynb
│   ├── 04_cycle_time_analysis.ipynb
│   ├── 05_quality_analysis.ipynb
│   ├── 06_bottleneck_analysis.ipynb
│   └── 07_pareto_analysis.ipynb
│
├── src/
│   ├── data_cleaning.py
│   ├── kpi.py
│   ├── analysis.py
│   └── utils.py
│
├── dashboard/
│   └── powerbi_dashboard.pbix
│
├── reports/
│   └── final_report.pdf
│
├── requirements.txt
└── README.md
```

---

## ⚙️ Workflow

1. Data Collection
2. Data Cleaning & Integration
3. KPI Calculation (OEE Framework)
4. Diagnostic Analysis
5. Insight Generation
6. Dashboard Visualization
7. Business Recommendations

---

## 📊 Key Performance Indicators (KPIs)

The project uses industry-standard OEE metrics:

* **Availability** → Machine running time vs planned time
* **Performance** → Actual vs ideal cycle time
* **Quality** → Good units vs total production
* **OEE** → Combined efficiency metric

---

## 🔍 Key Analyses Performed

### 1. Machine Efficiency Analysis

* Compared machine utilization
* Identified underperforming machines

### 2. Downtime Analysis

* Categorized downtime:

  * Planned
  * Unplanned
  * Minor stoppages

👉 Major losses came from **frequent small stoppages**

---

### 3. Cycle Time Analysis

* Analyzed variation across shifts

👉 Night shift showed **higher variability**

---

### 4. Quality Analysis

* Studied rejection rates across machines

👉 Specific machines showed **consistent defects**

---

### 5. Bottleneck Analysis

* Identified slowest process

👉 One machine was limiting overall throughput

---

### 6. Pareto Analysis

* Identified top contributors to downtime

👉 Top issues contributed to **~70% of losses**

---

## 💡 Key Insights

* Hidden losses due to **minor stoppages**
* **Shift-based performance gap** (night shift underperformance)
* **Bottleneck machine** limiting output
* Strong link between **machine condition & quality defects**
* Maintenance strategy was **reactive, not targeted**

---

## 🚀 Business Recommendations

* Track and reduce **minor stoppages**
* Optimize or upgrade **bottleneck machine**
* Improve **night shift supervision & training**
* Implement **regular calibration schedules**
* Focus on **high-impact maintenance issues (Pareto-based)**

---

## 📈 Expected Impact

* OEE improvement: **+10–18%**
* Downtime reduction: **20–30%**
* Rejection rate reduction: **10–15%**
* Production output increase: **12–20%**

---

## 🛠️ Tech Stack

* **Python** (Pandas, NumPy, Matplotlib, Seaborn)
* **Jupyter Notebooks**
* **Power BI / Tableau**
* **Excel (optional)**

---

## 📊 Dashboard

The dashboard includes:

* OEE trend analysis
* Downtime breakdown
* Machine performance comparison
* Shift-based insights
* Pareto charts

---

## 🧪 How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run notebooks
jupyter notebook
```

---

## 📌 Key Highlights

✔ Real-world manufacturing use case
✔ Strong focus on business impact
✔ Diagnostic (not generic ML project)
✔ Clean and modular code structure
✔ End-to-end analytical storytelling

---

## 📖 Learnings

* Importance of **data quality in industrial systems**
* Real-world application of **OEE metrics**
* Identifying **hidden inefficiencies**
* Translating data into **actionable business decisions**

---

## 🔮 Future Enhancements (Optional)

* Real-time data integration
* Automated dashboards
* Predictive maintenance (future scope)

---

## 👤 Author

**Prathamesh Pawar**
Data Analyst | Engineering + Analytics

---

## ⭐ Final Note

This project demonstrates how **data analysis alone (without ML)** can drive **significant operational improvements** in manufacturing systems.

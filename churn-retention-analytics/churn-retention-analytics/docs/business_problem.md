# Customer Churn Early Warning & Retention Intelligence System

## 1. Business Context

In subscription and repeat-purchase businesses, customer churn directly impacts
revenue stability and growth. Most organizations react to churn after revenue
is already lost, rather than proactively identifying at-risk customers.

This project builds an end-to-end analytics system to:

- Detect early churn signals
- Quantify revenue exposure
- Prioritize customers for retention
- Enable proactive intervention

---

## 2. Business Model Assumption

**Model:** Repeat purchase / subscription hybrid

Revenue is generated when customers place orders or renew subscriptions.
Customer engagement and purchase frequency are leading indicators of retention.

---

## 3. Problem Statement

The business currently lacks:

- Early visibility into churn risk
- Quantification of revenue at risk
- Customer-level prioritization for retention
- Cohort-based retention monitoring

As a result, retention efforts are reactive and inefficient.

---

## 4. Churn Definition (Config-Driven)

A customer is considered **churned** when:

> No purchase activity for `churn_inactivity_days` (from config table)

This definition is intentionally configurable to support different business models.

---

## 5. Grain of Analysis

**Primary grain:** One row per customer

All intelligence, scoring, and alerts are generated at the customer level.

---

## 6. Key Business KPIs

- Customer churn rate
- Revenue at risk
- Retention rate by cohort
- High-risk customer count
- Recoverable revenue estimate

---

## 7. Analytical Objectives

This system aims to:

1. Build a normalized customer intelligence foundation  
2. Identify statistically validated churn drivers  
3. Segment customers using RFM methodology  
4. Quantify revenue exposure  
5. Generate explainable churn risk scores  
6. Produce proactive retention alerts  

---

## 8. Success Criteria

The project is successful if it enables the business to:

- Identify high-risk customers early
- Prioritize retention outreach
- Estimate recoverable revenue
- Monitor cohort retention trends
- Support executive decision-making

---

## 9. Assumptions

- Historical order data is complete
- Customer IDs are stable across tables
- Timezone differences are negligible
- Refund behavior is minimal (not modeled separately)

These assumptions are documented and can be revisited in production.
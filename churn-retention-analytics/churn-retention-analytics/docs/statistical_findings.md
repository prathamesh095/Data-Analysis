# Statistical Validation of Churn Drivers

## Objective

Validate which customer behaviors are significantly associated with churn.

---

## Tests Performed

### 1. Point-Biserial Correlation

**Purpose:** Measure relationship between churn flag and continuous features.

**Key Findings**

- Recency shows strong positive correlation with churn
- Frequency shows negative correlation
- Revenue contribution moderately correlated

**Business Interpretation**

Customers who have not purchased recently are significantly more likely to churn.

---

### 2. Two-Proportion Z-Test

**Purpose:** Compare churn rates across customer segments.

**Result**

Statistically significant difference observed between high-value and low-value customers.

**Implication**

Retention strategy should be segment-specific.

---

### 3. Mann–Whitney U Test

**Purpose:** Compare revenue distributions between risk groups.

**Result**

High-risk customers show significantly lower median revenue.

**Implication**

Risk scoring aligns with actual customer value behavior.

---

## Conclusion

Statistical evidence supports the selected churn drivers and validates the rule-based risk engine.
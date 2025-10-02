WITH qtr_correction AS (
    SELECT
        edition_id,
        ad_category,
        CASE
            WHEN quarter LIKE 'Q%-____' THEN CONCAT(SUBSTRING_INDEX(quarter, '-', -1), '-Q', SUBSTRING(quarter, 2, 1))
            WHEN quarter LIKE '1st Qtr %' THEN CONCAT(SUBSTRING(quarter, -4), '-Q1')
            WHEN quarter LIKE '2nd Qtr %' THEN CONCAT(SUBSTRING(quarter, -4), '-Q2')
            WHEN quarter LIKE '3rd Qtr %' THEN CONCAT(SUBSTRING(quarter, -4), '-Q3')
            WHEN quarter LIKE '4th Qtr %' THEN CONCAT(SUBSTRING(quarter, -4), '-Q4')
            ELSE quarter
        END AS quarter_standardized,
        ad_revenue
    FROM fact_ad_revenue
),
category_yearly_revenue AS (
    SELECT
        SUBSTRING_INDEX(qc.quarter_standardized, '-', 1) AS year,
        dc.standard_ad_category AS category_name,
        SUM(qc.ad_revenue) AS category_revenue
    FROM qtr_correction qc
    JOIN dim_ad_category dc ON qc.ad_category = dc.ad_category_id
    GROUP BY year, category_name
),
category_with_total AS (
    SELECT
        *,
        SUM(category_revenue) OVER (PARTITION BY year) AS total_revenue_year,
        ROUND(category_revenue / SUM(category_revenue) OVER (PARTITION BY year) * 100, 2) AS pct_of_year_total
    FROM category_yearly_revenue
)
SELECT
    year,
    category_name,
    ROUND(category_revenue, 2) AS category_revenue,
    ROUND(total_revenue_year, 2) AS total_revenue_year,
    pct_of_year_total
FROM category_with_total
WHERE pct_of_year_total > 50;
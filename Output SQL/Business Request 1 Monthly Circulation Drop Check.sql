WITH monthly_data AS (
    SELECT
        c.city AS city_name,
        DATE_FORMAT(STR_TO_DATE(f.Month, '%Y-%m-%d'), '%Y-%m-01') AS start_date,
        SUM(f.Net_Circulation) AS net_circulation
    FROM fact_print_sales f
    JOIN dim_city c 
        ON f.City_ID = c.City_ID
    WHERE f.Month BETWEEN '2019-01-01' AND '2024-12-31'
    GROUP BY c.city, DATE_FORMAT(STR_TO_DATE(f.Month, '%Y-%m-%d'), '%Y-%m-01')
),
with_diff AS (
    SELECT
        city_name,
        DATE_FORMAT(start_date, '%Y-%m') AS month,
        net_circulation,
        LAG(net_circulation) OVER (PARTITION BY city_name ORDER BY start_date) AS prev_net_circulation,
        (net_circulation - LAG(net_circulation) OVER (PARTITION BY city_name ORDER BY start_date)) AS abs_change
    FROM monthly_data
)
SELECT
    city_name,
    month,
    net_circulation
FROM with_diff
WHERE abs_change < 0   -- only drops
ORDER BY abs_change ASC   -- sharpest drops first
LIMIT 3;

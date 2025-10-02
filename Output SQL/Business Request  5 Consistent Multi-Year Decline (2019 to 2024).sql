WITH edition_city AS (
    -- Map edition to city (unique per edition)
    SELECT edition_ID, City_ID
    FROM fact_print_sales
    GROUP BY edition_ID, City_ID
),

print_yearly AS (
    -- Aggregate yearly net circulation per city
    SELECT 
        City_ID,
        YEAR(STR_TO_DATE(Month, '%Y-%m-%d %H:%i:%s')) AS year,
        SUM(Net_Circulation) AS yearly_net_circulation
    FROM fact_print_sales
    WHERE YEAR(STR_TO_DATE(Month, '%Y-%m-%d %H:%i:%s')) BETWEEN 2019 AND 2024
    GROUP BY City_ID, year
),

ad_yearly AS (
    -- Aggregate yearly ad revenue per city (normalized to INR)
    SELECT 
        ec.City_ID,
        CASE 
            WHEN quarter LIKE '%Qtr%' THEN CAST(RIGHT(quarter, 4) AS UNSIGNED)
            WHEN LEFT(quarter, 1) = 'Q' OR LEFT(quarter, 3) = '4th' THEN CAST(RIGHT(quarter, 4) AS UNSIGNED)
            ELSE CAST(LEFT(quarter, 4) AS UNSIGNED)
        END AS year,
        SUM(ad_revenue * 
            CASE 
                WHEN currency = 'USD' THEN 80 
                WHEN currency = 'EUR' THEN 90 
                WHEN currency IN ('INR', 'IN RUPEES') THEN 1 
                ELSE 1  -- Default to INR if unknown
            END
        ) AS yearly_ad_revenue
    FROM fact_ad_revenue ar
    JOIN edition_city ec ON ar.edition_id = ec.edition_ID
    WHERE 
        CASE 
            WHEN quarter LIKE '%Qtr%' THEN CAST(RIGHT(quarter, 4) AS UNSIGNED)
            WHEN LEFT(quarter, 1) = 'Q' OR LEFT(quarter, 3) = '4th' THEN CAST(RIGHT(quarter, 4) AS UNSIGNED)
            ELSE CAST(LEFT(quarter, 4) AS UNSIGNED)
        END BETWEEN 2019 AND 2024
    GROUP BY ec.City_ID, year
),

combined AS (
    -- Combine metrics per city-year
    SELECT 
        p.City_ID,
        d.city AS city_name,
        p.year,
        p.yearly_net_circulation,
        a.yearly_ad_revenue
    FROM print_yearly p
    JOIN ad_yearly a ON p.City_ID = a.City_ID AND p.year = a.year
    JOIN dim_city d ON p.City_ID = d.city_id
),

combined_with_lags AS (
    -- Add lags for previous values
    SELECT 
        *,
        LAG(yearly_net_circulation) OVER (PARTITION BY City_ID ORDER BY year) AS prev_circ,
        LAG(yearly_ad_revenue) OVER (PARTITION BY City_ID ORDER BY year) AS prev_ad
    FROM combined
),

decline_flags AS (
    -- Compute flags per city
    SELECT 
        city_name,
        year,
        yearly_net_circulation,
        yearly_ad_revenue,
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY City_ID) = 6 
                 AND MAX(year) OVER (PARTITION BY City_ID) - MIN(year) OVER (PARTITION BY City_ID) = 5
                 AND MIN(CASE WHEN prev_circ IS NOT NULL 
                              THEN IF(yearly_net_circulation < prev_circ, 1, 0) 
                              ELSE 1 END) OVER (PARTITION BY City_ID) = 1 
            THEN 'Yes' ELSE 'No' 
        END AS is_declining_print,
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY City_ID) = 6 
                 AND MAX(year) OVER (PARTITION BY City_ID) - MIN(year) OVER (PARTITION BY City_ID) = 5
                 AND MIN(CASE WHEN prev_ad IS NOT NULL 
                              THEN IF(yearly_ad_revenue < prev_ad, 1, 0) 
                              ELSE 1 END) OVER (PARTITION BY City_ID) = 1 
            THEN 'Yes' ELSE 'No' 
        END AS is_declining_ad_revenue
    FROM combined_with_lags
)

-- Final output: per city-year for cities declining in both
SELECT 
    city_name,
    year,
    yearly_net_circulation,
    yearly_ad_revenue,
    is_declining_print,
    is_declining_ad_revenue,
    CASE WHEN is_declining_print = 'Yes' AND is_declining_ad_revenue = 'Yes' THEN 'Yes' ELSE 'No' END AS is_declining_both
FROM decline_flags
WHERE is_declining_print = 'Yes' AND is_declining_ad_revenue = 'Yes'
ORDER BY city_name, year;
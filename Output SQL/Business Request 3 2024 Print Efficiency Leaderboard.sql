SELECT  
    c.city,
    SUM(fs.Copies_Sold + fs.Copies_Returned) AS copies_printed_2024,
    SUM(fs.Net_Circulation) AS net_circulation_2024,
    ROUND(
        (SUM(fs.Net_Circulation) / NULLIF(SUM(fs.Copies_Sold + fs.Copies_Returned), 0)) * 100,
        2
    ) AS efficiency_ratio,
    RANK() OVER (
        ORDER BY 
            ROUND(
                (SUM(fs.Net_Circulation) / NULLIF(SUM(fs.Copies_Sold + fs.Copies_Returned), 0)) * 100,
                2
            ) DESC
    ) AS efficiency_rank_2024
FROM  
    fact_print_sales fs
JOIN  
    dim_city c ON fs.City_ID = c.City_ID
WHERE  
    YEAR(STR_TO_DATE(fs.Month, '%Y-%m-%d')) = 2024
GROUP BY  
    c.city;

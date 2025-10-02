WITH readiness_2021 as(
	select
		city_id,
        avg(literacy_rate) as avg_Literacy_rate,
        avg(smartphone_penetration) as avg_smartPhone_penetration,
        avg(internet_penetration) as avg_internet_penetration,
        round((avg(literacy_rate) + avg(smartphone_penetration) + avg(internet_penetration)) / 3 , 2) as readiness_score_2021
	from fact_city_readiness
	where quarter like '2021-Q%'
    group by city_id
),
engagement_2021 as (
	select
		city_id,
        round(100 - (SUM(avg_bounce_rate * users_reached) / SUM(users_reached)) ,2) AS engagement_metric_2021
	from 
		fact_digital_pilot
	group by city_id
),
combined as(
	select
		r.city_ID,
        d.city as city_name,
        r.readiness_score_2021,
        e.engagement_metric_2021
	from readiness_2021 r 
	join engagement_2021 e on r.city_id = e.city_id
    join dim_city d on d.city_id = r.city_id
),
ranked AS (
    -- Add ranks
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY readiness_score_2021 DESC) AS readiness_rank_desc,
        ROW_NUMBER() OVER (ORDER BY engagement_metric_2021 ASC) AS engagement_rank_asc
    FROM combined
)
select
	city_name, 
    readiness_score_2021, 
    engagement_metric_2021, 
    readiness_rank_desc, 
    engagement_rank_asc,
    'Yes' as is_oulier
from ranked
where readiness_rank_desc = 1 and engagement_rank_asc <= 3
order by city_name
With readiness  as (
	select
		c.city,
		max(case when f.quarter = '2021-Q1' then internet_penetration end) as internet_rate_q1_24,
		max(case when f.quarter =  '2021-Q4' then internet_penetration end) as internet_rate_q1_25
	from
		fact_city_readiness f
	join dim_city c on f.city_id = c.city_id
	where f.quarter in ('2021-Q1' ,'2021-Q4')
	group by c.city
)
select
	*,
    round((internet_rate_q1_25 - internet_rate_q1_24),2) as delta_internet_rate,
    rank() over(order by (internet_rate_q1_25 - internet_rate_q1_24) DESC) as improvement_rank
from readiness 
    
select *

from
(SELECT
date_parse("date",'%Y-%m-%d') as reporting_date,
fips,
county,
cases,
try(cast(cases as int))-coalesce(try(cast(lag(cases,1) OVER(PARTITION BY fips ORDER BY date_parse("date",'%Y-%m-%d') asc) as int)),0) as new_cases,
try(cast(deaths as int))-coalesce(try(cast(lag(deaths,1) OVER(PARTITION BY fips ORDER BY date_parse("date",'%Y-%m-%d') asc) as int)),0) as new_deaths,
state

FROM "covid-19"."nytimes_counties"

where state in ('New York','New Jersey','Washington')

order by 1 asc, 2
)
 where reporting_date >= date '2021-06-01'

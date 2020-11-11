create materialized view `table-loader-dev.static_data.d_country_mview`
as
select country_iso_code, count(1) as c
from `table-loader-dev.static_data.d_country`
group by country_iso_code

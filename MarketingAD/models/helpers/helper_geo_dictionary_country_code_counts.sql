select
    country_code,
    count(*) as cnt
from {{ ref('stg_geo_dictionary') }}
group by country_code
having count(*) > 1


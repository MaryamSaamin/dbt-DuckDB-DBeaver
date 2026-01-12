select digits_only, count(*) 
from {{ ref('stg_geo_dictionary') }}
where digits_only is not null
group by digits_only
having count(*) > 1

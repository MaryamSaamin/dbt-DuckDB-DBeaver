with mart as (
    select *
    from {{ ref('mart_fact_network_report') }}
)

select count(*) as total_rows
from mart

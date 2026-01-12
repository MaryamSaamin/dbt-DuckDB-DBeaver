with raw as (
    select *
    from {{ ref('raw_ae_ad_network_2_report') }}
),

with_country_code as (
    select
        *,
        regexp_extract(campaign_name, 'iOS_([A-Z]{2})_', 1) as country_code
    from raw
)

select
    r.*,
    c.country_code_id
from with_country_code r
left join {{ ref('dim_country') }} c
    on r.country_code = c.country_code



with c as (
    select
        campaign_id,
        update_date
    from {{ ref('stg_ae_ad_network_1_country_report') }}
),

d as (
    select
        campaign_id,
        update_date
    from {{ ref('stg_ae_ad_network_1_detailed_report') }}
)

select
    count(case when c.campaign_id is not null and d.campaign_id is not null then 1 end)
        as same_campaign_date,

    count(case when c.campaign_id is not null and d.campaign_id is null then 1 end)
        as only_in_country_report,

    count(case when c.campaign_id is null and d.campaign_id is not null then 1 end)
        as only_in_detailed_report

from c
full outer join d
    on c.campaign_id = d.campaign_id
   and c.update_date = d.update_date

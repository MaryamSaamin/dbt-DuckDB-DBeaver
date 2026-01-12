select campaign_id, update_date, count(*)
from {{ ref('raw_ae_ad_network_1_campaign_updates') }}
group by campaign_id, update_date
having count(*) > 1

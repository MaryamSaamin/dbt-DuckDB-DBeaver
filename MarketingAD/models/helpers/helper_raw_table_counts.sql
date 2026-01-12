select
    'raw_ae_ad_network_1_campaign_updates' as table_name,
    count(*) as row_count
from {{ ref('raw_ae_ad_network_1_campaign_updates') }}

union all

select
    'raw_ae_ad_network_1_country_report' as table_name,
    count(*) as row_count
from {{ ref('raw_ae_ad_network_1_country_report') }}

union all

select
    'raw_ae_ad_network_1_detailed_report' as table_name,
    count(*) as row_count
from {{ ref('raw_ae_ad_network_1_detailed_report') }}

union all

select
    'raw_ae_ad_network_1_geo_dictionary' as table_name,
    count(*) as row_count
from {{ ref('raw_ae_ad_network_1_geo_dictionary') }}

union all

select
    'raw_ae_ad_network_2_report' as table_name,
    count(*) as row_count
from {{ ref('raw_ae_ad_network_2_report') }}

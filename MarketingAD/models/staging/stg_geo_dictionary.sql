with raw as (
    select *
    from {{ ref('raw_ae_ad_network_1_geo_dictionary') }}
),

-- Classify location_name
classified as (
    select
        *,
        case
            when location_name ~ '^[0-9]+$' then cast(location_name as bigint)
            else null
        end as digits_only,
        case
            when location_name ~ '^[A-Za-z]+$' then location_name
            else null
        end as letters_only,
        case
            when location_name ~ '[A-Za-z]' and location_name ~ '[0-9]' then location_name
            else null
        end as letters_and_digits
    from raw
),

-- Ensure UK exists
geo_extended as (
    select *
    from classified

    union all

    select
        max(location_id) + 1 as location_id,
        'UK' as country_code,
        'United Kingdom' as location_name,
        'Country' as location_type,
        null as digits_only,
        null as letters_only,
        null as letters_and_digits
    from classified
    where not exists (
        select 1
        from classified
        where country_code = 'UK'
    )
),

-- Generate ONE country_code_id per country_code
country_ids as (
    select
        country_code,
        row_number() over (order by country_code) as country_code_id
    from geo_extended
    group by country_code
),

-- Attach country_code_id back to all rows
final as (
    select
        g.*,
        c.country_code_id
    from geo_extended g
    join country_ids c
        on g.country_code = c.country_code
)

select *
from final

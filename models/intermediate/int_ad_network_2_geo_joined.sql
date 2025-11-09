with geo_dim as (
    select
        location_id   as country_id,
        lower(trim(country_code)) as country_code,
        trim(location_name) as country_name
    from {{ ref('int_geo_dim_extension') }}
    where lower(location_type) in ('country', 'region')
),

network2 as (
    select
        campaign_id,
        campaign_name,
        lower(trim(country_code)) as country_code,
        report_date,
        spend,
        impressions,
        clicks
    from {{ ref('int_ad_network_2_enriched') }}
)

select
    'Network 2' as network_source,
    n.report_date,
    n.campaign_id,
    n.campaign_name,
    'NA' as state_id,
    'NA' as state_name,
    g.country_id,
    g.country_name,
    g.country_code,
    'country' as location_type,
    n.spend,
    n.impressions,
    n.clicks,
    'country' as geo_level
from network2 n
left join geo_dim g
    on n.country_code = g.country_code

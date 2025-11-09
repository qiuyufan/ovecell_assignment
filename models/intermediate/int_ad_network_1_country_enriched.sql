-- ============================================================
-- Ad Network 1 – Country-Level Enriched Model (Final DBT Version)
-- ============================================================

with country_report as (
    select
        report_date,
        campaign_id,
        country_id,
        spend,
        impressions,
        clicks
    from {{ ref('stg_ad_network_1_country_report') }}
),

campaign_meta as (
    select distinct
        campaign_id,
        trim(campaign_name) as campaign_name
    from {{ ref('stg_ad_network_1_campaign_updates') }}
),

geo_country as (
    select
        location_id   as geo_country_id,
        trim(location_name) as geo_country_name,
        lower(country_code) as geo_country_code,
        lower(location_type) as location_type
    from {{ ref('int_geo_dim_extension') }}
)

select
    f.report_date,
    f.campaign_id,
    c.campaign_name,
    g.geo_country_id   as country_id,
    g.geo_country_name as country_name,
    g.geo_country_code as country_code,
    g.location_type as location_type,
    f.spend,
    f.impressions,
    f.clicks
from country_report f
left join campaign_meta c
    on f.campaign_id = c.campaign_id
left join geo_country g
    on cast(f.country_id as varchar) = cast(g.geo_country_id as varchar)
   and lower(g.location_type) in ('country', 'region')

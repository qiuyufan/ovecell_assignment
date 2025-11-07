{{ config(materialized='table') }}

-- ============================================================
-- Ad Network 2 – Joined with Geo Dimension
--
-- Purpose:
--   Combine Ad Network 2 performance data (with derived country_code)
--   with the geo dictionary to attach full country names and IDs.
--
-- ============================================================

with geo_dim as (
    select
        location_id,
        lower(trim(country_code)) as country_code,
        trim(location_name) as country_name
    from {{ ref('stg_ad_network_1_geo_dictionary') }}
    where lower(location_type) = 'country'
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
    n.campaign_id,
    n.campaign_name,
    n.report_date,
    n.country_code,
    g.country_name,
    g.location_id as country_id,
    n.spend,
    n.impressions,
    n.clicks
from network2 n
left join geo_dim g
    on n.country_code = g.country_code

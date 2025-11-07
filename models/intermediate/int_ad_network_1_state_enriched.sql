{{ config(materialized='table') }}

-- ============================================================
-- Ad Network 1 – State-Level Enriched Model
--
-- Purpose:
--   Combine detailed (state-level) report with campaign
--   metadata and geographic info from int_geo_dim_extension.
-- ============================================================

with detailed as (
    select
        report_date,
        campaign_id,
        country_id,
        state_id,
        spend,
        impressions,
        clicks
    from {{ ref('stg_ad_network_1_detailed_report') }}
),

campaign_meta as (
    select
        campaign_id,
        max(campaign_name) as campaign_name
    from {{ ref('stg_ad_network_1_campaign_updates') }}
    group by campaign_id
),

geo_state as (
    select
        location_id   as state_id,
        trim(location_name) as state_name,
        lower(country_code) as state_country_code
    from {{ ref('int_geo_dim_extension') }}
    where lower(location_type) = 'state'
),

geo_country as (
    select
        location_id   as country_id,
        trim(location_name) as country_name,
        lower(country_code) as country_code
    from {{ ref('int_geo_dim_extension') }}
    where lower(location_type) = 'country'
)

select
    d.report_date,
    d.campaign_id,
    cm.campaign_name,
    s.state_id,
    s.state_name,
    c.country_id,
    c.country_name,
    coalesce(s.state_country_code, c.country_code) as country_code,
    d.spend,
    d.impressions,
    d.clicks
from detailed d
left join campaign_meta cm
    on d.campaign_id = cm.campaign_id
left join geo_state s
    on d.state_id = s.state_id
left join geo_country c
    on d.country_id = c.country_id

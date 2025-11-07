{{ config(materialized='table') }}

-- ============================================================
-- Ad Network 1 – Country-Level Enriched Model
--
-- Purpose:
--   Enrich the country-level performance data with campaign
--   metadata and geographic information.
--
-- Inputs:
--   - stg_ad_network_1_country_report
--   - stg_ad_network_1_campaign_updates
--   - stg_ad_network_1_geo_dictionary
--
-- Outputs:
--   - report_date, campaign_id, campaign_name
--   - country_id, country_name
--   - spend, impressions, clicks
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
    select
        campaign_id,
        max(campaign_name) as campaign_name  -- if multiple update_dates exist
    from {{ ref('stg_ad_network_1_campaign_updates') }}
    group by campaign_id
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
    c.report_date,
    c.campaign_id,
    cm.campaign_name,
    g.country_id,
    g.country_name,
    g.country_code,
    c.spend,
    c.impressions,
    c.clicks
from country_report c
left join campaign_meta cm
    on c.campaign_id = cm.campaign_id
left join geo_country g
    on c.country_id = g.country_id

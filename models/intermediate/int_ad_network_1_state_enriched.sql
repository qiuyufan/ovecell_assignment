
-- ============================================================
-- Ad Network 1 – State-Level Enriched Model (Final)
--
-- Purpose:
--   Enrich the Ad Network 1 state-level report with:
--     • Campaign name (1-to-1 by campaign_id)
--     • Hierarchical geo information (state + country)
--
-- Key Design:
--   - Keeps all rows from the detailed report (true LEFT JOIN)
--   - Filters geo rows inside join conditions to avoid row loss
--   - Simplified join: each campaign_id has one campaign_name
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
    select distinct
        campaign_id,
        trim(campaign_name) as campaign_name
    from {{ ref('stg_ad_network_1_campaign_updates') }}
),

geo_dim as (
    select
        location_id,
        trim(location_name) as location_name,
        lower(country_code) as country_code,
        lower(location_type) as location_type
    from {{ ref('int_geo_dim_extension') }}
)

select
    f.report_date,
    f.campaign_id,
    c.campaign_name,
    s.location_id      as state_id,
    s.location_name    as state_name,
    g.location_id      as country_id,
    g.location_name    as country_name,
    coalesce(s.country_code, g.country_code) as country_code,
    g.location_type    as location_type,
    f.spend,
    f.impressions,
    f.clicks
from detailed f
left join campaign_meta c
    on f.campaign_id = c.campaign_id
left join geo_dim s
    on cast(f.state_id as varchar) = cast(s.location_id as varchar)
   and s.location_type = 'state'
left join geo_dim g
    on cast(f.country_id as varchar) = cast(g.location_id as varchar)
   and g.location_type = 'country'

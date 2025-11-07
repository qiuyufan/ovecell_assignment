{{ config(materialized='table') }}

-- ============================================================
-- Marketing Ad Performance Datamart
--
-- Purpose:
--   Unified view of Ad Network 1 and Ad Network 2 performance
--   at the most granular geo level available.
-- ============================================================

with ad1 as (
    select
        'ad_network_1' as ad_network,
        report_date,
        campaign_id,
        campaign_name,
        country_name,
        country_code,
        state_name,
        spend,
        impressions,
        clicks
    from {{ ref('int_ad_network_1_combined') }}
),

ad2 as (
    select
        'ad_network_2' as ad_network,
        report_date,
        campaign_id,
        campaign_name,
        country_name,
        country_code,
        null as state_name,
        spend,
        impressions,
        clicks
    from {{ ref('int_ad_network_2_geo_joined') }}
)

select * from ad1
union all
select * from ad2

{{ config(materialized='table') }}

-- ============================================================
-- Ad Network 1 – Combined Country + State Table
--
-- Purpose:
--   Merge the state-level and country-level enriched data
--   into a single unified fact table with consistent structure.
--
-- Logic:
--   1. Take all state-level rows (most detailed granularity)
--   2. Add country-level rows for countries without any state-level data
-- ============================================================

with state_data as (
    select
        report_date,
        campaign_id,
        campaign_name,
        state_id,
        state_name,
        country_id,
        country_name,
        country_code,
        spend,
        impressions,
        clicks
    from {{ ref('int_ad_network_1_state_enriched') }}
),

country_data as (
    select
        report_date,
        campaign_id,
        campaign_name,
        null as state_id,
        null as state_name,
        country_id,
        country_name,
        country_code,
        spend,
        impressions,
        clicks
    from {{ ref('int_ad_network_1_country_enriched') }}
    where country_id not in (
        select distinct country_id from {{ ref('int_ad_network_1_state_enriched') }}
    )
)

select * from state_data
union all
select * from country_data

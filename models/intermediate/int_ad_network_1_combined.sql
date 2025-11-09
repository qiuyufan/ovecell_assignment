-- ============================================================
-- Ad Network 1 – Combined (Country + State, Union-Ready)
-- ============================================================

with state_data as (
    select
        'Network 1' as network_source,
        report_date,
        campaign_id,
        campaign_name,
        state_id,
        state_name,
        country_id,
        country_name,
        country_code,
        location_type,      -- keep this before spend to match Network 2
        spend,
        impressions,
        clicks,
        'state' as geo_level
    from {{ ref('int_ad_network_1_state_enriched') }}
),

country_data as (
    select
        'Network 1' as network_source,
        report_date,
        campaign_id,
        campaign_name,
        'NA' as state_id,
        'NA' as state_name,
        country_id,
        country_name,
        country_code,
        location_type,
        spend,
        impressions,
        clicks,
        'country' as geo_level
    from {{ ref('int_ad_network_1_country_enriched') }}
)

select * from state_data
union all
select * from country_data

/* ============================================================
   VALIDATION CHECKS – Ovecell Assignment
   Author: Qiuyu Fan
   Purpose: Manual validation of staging → int → mart layers
   ============================================================ */


/* ============================================================
   🧩 1. STAGING LAYER CHECKS
   ------------------------------------------------------------
   Verify that all foreign keys and geo references exist,
   and identify any missing or unexpected location types.
   ============================================================ */

-- 1.1 Country IDs in staging fact missing in geo dictionary
select distinct f.country_id
from main.stg_ad_network_1_country_report f
left join main.stg_ad_network_1_geo_dictionary g
  on cast(f.country_id as varchar) = cast(g.location_id as varchar)
where g.location_id is null
  and f.country_id is not null;

-- 1.2 Count match between staging fact IDs and geo dictionary
select
  'country_report (country_id)' as source_table,
  count(distinct f.country_id) as fact_ids,
  count(distinct g.location_id) filter (where g.location_id is not null) as matched_ids
from main.stg_ad_network_1_country_report f
left join main.stg_ad_network_1_geo_dictionary g
  on cast(f.country_id as varchar) = cast(g.location_id as varchar)

union all

select
  'detailed_report (state_id)',
  count(distinct f.state_id),
  count(distinct g.location_id) filter (where g.location_id is not null)
from main.stg_ad_network_1_detailed_report f
left join main.stg_ad_network_1_geo_dictionary g
  on cast(f.state_id as varchar) = cast(g.location_id as varchar)

union all

select
  'detailed_report (country_id)',
  count(distinct f.country_id),
  count(distinct g.location_id) filter (where g.location_id is not null)
from main.stg_ad_network_1_detailed_report f
left join main.stg_ad_network_1_geo_dictionary g
  on cast(f.country_id as varchar) = cast(g.location_id as varchar);

-- 🧠 Note: 3 missing IDs identified earlier and added to geo dimension.

-- 1.3 Network 2 – missing country codes in geo dictionary
select distinct f.country_code
from main.int_ad_network_2_enriched f
left join main.stg_ad_network_1_geo_dictionary g
  on lower(trim(f.country_code)) = lower(trim(g.country_code))
where g.country_code is null
  and f.country_code is not null;
-- (UK was identified and added.)

-- 1.4 Non-country location types (regions) in geo dictionary
select distinct
    g.location_id,
    g.location_name,
    g.location_type,
    g.country_code
from main.stg_ad_network_1_country_report f
left join main.stg_ad_network_1_geo_dictionary g
  on cast(f.country_id as varchar) = cast(g.location_id as varchar)
where lower(g.location_type) <> 'country'
  and g.location_id is not null
order by g.location_type, g.location_name;


/* ============================================================
   ⚙️ 2. INTERMEDIATE (INT) LAYER CHECKS
   ------------------------------------------------------------
   Ensure no data loss or duplication after enrichment joins.
   ============================================================ */

-- 2.1 Compare row counts between staging and intermediate
select
  'country_report' as source,
  (select count(*) from main.stg_ad_network_1_country_report) as staging_rows,
  (select count(*) from main.int_ad_network_1_country_enriched) as int_rows
union all
select
  'detailed_report' as source,
  (select count(*) from main.stg_ad_network_1_detailed_report),
  (select count(*) from main.int_ad_network_1_state_enriched);

-- 2.2 Null check for geo joins
select *
from main.int_ad_network_1_country_enriched
where country_id is null
limit 20;

select *
from main.int_ad_network_1_state_enriched
where state_id is null
   or country_id is null
limit 20;

-- 2.3 Duplicate check
select campaign_id, report_date, country_id, count(*) as cnt
from main.int_ad_network_1_country_enriched
group by 1, 2, 3
having count(*) > 1;

select campaign_id, report_date, state_id, count(*) as cnt
from main.int_ad_network_1_state_enriched
group by 1, 2, 3
having count(*) > 1;


/* ============================================================
   📊 3. FINAL MART CHECKS
   ------------------------------------------------------------
   Validate that both networks combine correctly and metrics match.
   ============================================================ */

-- 3.1 Record counts between INT and MART
select
  'Network 1 total (INT)' as source,
  (select count(*) from main.int_ad_network_1_combined) as n_rows_int,
  (select count(*) from main.mkt_ad_performance_mart where network_source = 'Network 1') as n_rows_mart
union all
select
  'Network 2 total (INT)',
  (select count(*) from main.int_ad_network_2_geo_joined),
  (select count(*) from main.mkt_ad_performance_mart where network_source = 'Network 2');

-- 3.2 Verify uniqueness per campaign/date/geo level
select
  network_source,
  campaign_id,
  report_date,
  geo_level,
  country_id,
  state_id,
  count(*) as cnt
from main.mkt_ad_performance_mart
group by 1,2,3,4,5,6
having count(*) > 1;

-- 3.3 Metric reconciliation between INT and MART
select
  'Network 1' as network,
  (select sum(spend) from main.int_ad_network_1_combined) as int_spend,
  (select sum(spend) from main.mkt_ad_performance_mart where network_source = 'Network 1') as mart_spend,
  (select sum(impressions) from main.int_ad_network_1_combined) as int_impr,
  (select sum(impressions) from main.mkt_ad_performance_mart where network_source = 'Network 1') as mart_impr,
  (select sum(clicks) from main.int_ad_network_1_combined) as int_clicks,
  (select sum(clicks) from main.mkt_ad_performance_mart where network_source = 'Network 1') as mart_clicks
union all
select
  'Network 2',
  (select sum(spend) from main.int_ad_network_2_geo_joined),
  (select sum(spend) from main.mkt_ad_performance_mart where network_source = 'Network 2'),
  (select sum(impressions) from main.int_ad_network_2_geo_joined),
  (select sum(impressions) from main.mkt_ad_performance_mart where network_source = 'Network 2'),
  (select sum(clicks) from main.int_ad_network_2_geo_joined),
  (select sum(clicks) from main.mkt_ad_performance_mart where network_source = 'Network 2');

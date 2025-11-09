{{ config(materialized='table') }}

with cleaned as (
  select
    location_id,
    country_code,
    location_name,
    location_type
  from {{ ref('stg_ad_network_1_geo_dictionary') }}
  where country_code is not null
    and lower(country_code) <> 'none'
),

manual_fixes as (
  select * from (
    values
      (2760, null, 'Unknown Country 2760', 'country'),
      (2192, null, 'Unknown Country 2192', 'country'),
      (2364, null, 'Unknown Country 2364', 'country'),
      (9999, 'UK', 'United Kingdom (manual)', 'country')
  ) as t(location_id, country_code, location_name, location_type)
)

select * from cleaned
union all
select * from manual_fixes

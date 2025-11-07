{{ config(materialized='table') }}

select * from {{ ref('stg_ad_network_1_geo_dictionary') }}

union all

select * from (
  values
    (2760, null, 'Unknown Country 2760', 'country'),
    (2192, null, 'Unknown Country 2192', 'country'),
    (2364, null, 'Unknown Country 2364', 'country')
) as t(location_id, country_code, location_name, location_type)
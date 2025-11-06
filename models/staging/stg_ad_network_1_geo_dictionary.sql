{{ config(materialized='table') }}

select
    id::varchar as location_id,
    trim(country_code) as country_code,
    trim(name) as location_name,
    trim(location_type) as location_type
from read_csv_auto('task/ae_ad_network_1_geo_dictionary.csv')

{{ config(materialized='table') }}

select * from {{ ref('int_ad_network_1_combined') }}
union all
select * from {{ ref('int_ad_network_2_geo_joined') }}

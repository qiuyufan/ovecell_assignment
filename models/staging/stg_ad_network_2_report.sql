{{ config(materialized='table') }}

select
  cast(date as date) as report_date,
  id::varchar as campaign_id,
  trim(campaign_name) as campaign_name,
  spend::float,
  impressions::int,
  clicks::int
from {{ source('ad_network_2', 'report') }}

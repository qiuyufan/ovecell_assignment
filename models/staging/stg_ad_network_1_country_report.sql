{{ config(materialized='table') }}

select
  cast(date as date) as report_date,
  campaign_id::varchar,
  country_id::varchar,
  null as state_id,
  spend::float,
  impressions::int,
  clicks::int
from {{ source('ad_network_1', 'country_report') }}

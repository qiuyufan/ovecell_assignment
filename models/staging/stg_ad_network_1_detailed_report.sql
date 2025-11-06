{{ config(materialized='table') }}

select
  cast(date as date) as report_date,
  campaign_id::varchar,
  country_id::varchar,
  state_id::varchar,
  spend::float,
  impressions::int,
  clicks::int
from {{ source('ad_network_1', 'detailed_report') }}

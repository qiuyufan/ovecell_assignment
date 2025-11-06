{{ config(materialized='table') }}

select
  campaign_id::varchar as campaign_id,
  cast(update_date as date) as update_date,
  trim(name) as campaign_name
from {{ source('ad_network_1', 'campaign_updates') }}

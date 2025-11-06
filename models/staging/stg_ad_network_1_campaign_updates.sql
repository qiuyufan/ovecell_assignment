{{ config(materialized='table') }}

select
    campaign_id::varchar as campaign_id,
    cast(update_date as date) as update_date,
    trim(name) as campaign_name
from read_csv_auto('task/ae_ad_network_1_campaign_updates.csv')

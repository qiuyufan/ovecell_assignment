{{ config(materialized='table') }}

select
    id::varchar as campaign_id,
    trim(campaign_name) as campaign_name,
    cast(date as date) as report_date,
    cast(spend as double) as spend,
    cast(impressions as bigint) as impressions,
    cast(clicks as bigint) as clicks
from read_csv_auto('task/ae_ad_network_2_report.csv')

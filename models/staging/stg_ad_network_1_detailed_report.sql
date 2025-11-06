{{ config(materialized='table') }}

select
    cast(date as date) as report_date,
    campaign_id::varchar as campaign_id,
    country_id::varchar as country_id,
    state_id::varchar as state_id,
    cast(spend as double) as spend,
    cast(impressions as bigint) as impressions,
    cast(clicks as bigint) as clicks
from read_csv_auto('task/ae_ad_network_1_detailed_report.csv')

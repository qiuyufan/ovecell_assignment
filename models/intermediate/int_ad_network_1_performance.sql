select * from {{ ref('stg_ad_network_1_detailed_report') }}
union all
select * from {{ ref('stg_ad_network_1_country_report') }}
where country_id not in (
    select distinct country_id from {{ ref('stg_ad_network_1_detailed_report') }}
)

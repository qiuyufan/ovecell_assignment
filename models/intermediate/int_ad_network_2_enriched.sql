{{ config(materialized='table') }}

-- ============================================================
-- Ad Network 2 – Enriched Model
--
-- Purpose:
--   Filters out 'None' campaign_name rows (verified duplicates)
--   and extracts country_code from campaign_name.
--
-- ============================================================

select
    r.*,
    regexp_extract(r.campaign_name, '_([A-Z]{2})_', 1) as country_code
from {{ ref('stg_ad_network_2_report') }} r
where lower(trim(campaign_name)) <> 'none'

-- depends_on: {{ref('ExchangeRates')}}

--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('SD_ProductAdsReport', True)) }}

{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'reportDate', 'data_type': 'date' },
    cluster_by = ['CampaignId', 'adGroupID', 'asin', 'sku'], 
    unique_key = ['reportDate','CampaignId', 'adGroupID', 'asin', 'sku'])}}
{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['reportDate','CampaignId', 'adGroupID', 'asin', 'sku'])}}
{% endif %}


{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%sponsoreddisplay_productadsreport' 
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% if var('timezone_conversion_flag')['amazon_ads'] %}
    {% set hr = var('timezone_conversion_hours') %}
{% endif %}

{% for i in results_list %}
    {% if var('brand_consolidation_flag') %}
        {% set id =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set id = var('brand_name') %}
    {% endif %}

    SELECT * except(row_num)
    From (
        select '{{id}}' as brand,
        cast(RequestTime as timestamp) RequestTime,
        tactic,
        profileId,        
        countryName,
        accountName,
        accountId,
        {% if var('timezone_conversion_flag')['amazon_ads'] %}
            cast(DATETIME_ADD(cast(reportDate as timestamp), INTERVAL {{hr}} HOUR ) as DATE) reportDate,
        {% else %}
            cast(reportDate as DATE) reportDate,
        {% endif %}
        adGroupName,
        adGroupId,
        asin,
        sku,
        adId,
        campaignName,
        campaignId,
        impressions,
        clicks,
        cost,
        currency,
        attributedConversions1d,
        attributedConversions7d,
        attributedConversions14d,
        attributedConversions30d,
        attributedConversions1dSameSKU,
        attributedConversions7dSameSKU,
        attributedConversions14dSameSKU,
        attributedConversions30dSameSKU,
        attributedUnitsOrdered1d,
        attributedUnitsOrdered7d,
        attributedUnitsOrdered14d,
        attributedUnitsOrdered30d, 
        attributedSales14d,
        attributedSales1dSameSKU,
        attributedSales7dSameSKU,
        attributedSales14dSameSKU,
        attributedSales30dSameSKU,
        attributedOrdersNewToBrand14d,	
        attributedSalesNewToBrand14d,	
        attributedUnitsOrderedNewToBrand14d,	
        attributedDetailPageView14d,	
        viewImpressions,	
        viewAttributedConversions14d,	
        viewAttributedSales14d,	
        viewAttributedUnitsOrdered14d,	
        viewAttributedDetailPageView14d,	
        attributedBrandedSearches14d,	
        viewAttributedBrandedSearches14d,	
        viewAttributedOrdersNewToBrand14d,	
        viewAttributedSalesNewToBrand14d,	
        viewAttributedUnitsOrderedNewToBrand14d,
        CURRENT_TIMESTAMP as updated_date,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code , 
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            cast(null as string) as exchange_currency_code ,
        {% endif %}
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        {% if var('timezone_conversion_flag')['amazon_ads'] %}
           DATETIME_ADD(cast(reportDate as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
           CAST(reportDate as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        DENSE_RANK() OVER (PARTITION BY reportDate,CampaignId, adGroupID,
        asin, sku order by a._daton_batch_runtime desc) row_num
        from {{i}} a
            {% if var('currency_conversion_flag') %} 
                left join {{ref('ExchangeRates')}} c on date(a.RequestTime) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a._daton_batch_runtime  >= {{max_loaded}}
            {% endif %}    
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
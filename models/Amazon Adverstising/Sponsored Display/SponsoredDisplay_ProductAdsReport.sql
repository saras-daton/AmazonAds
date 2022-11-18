{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'reportDate', 'data_type': 'date' },
    cluster_by = ['CampaignId', 'adGroupID', 'asin', 'sku'], 
    unique_key = ['reportDate','CampaignId', 'adGroupID', 'asin', 'sku'])}}

-- depends_on: {{ ref('ExchangeRates') }}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT MAX(_daton_batch_runtime) - 2592000000 FROM {{ this }}
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


{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[1] %}
    SELECT * except(row_num)
    From (
        select '{{id}}' as brand,
        cast(DATETIME_ADD(RequestTime, INTERVAL -7 HOUR ) as Date) RequestTime,	
        tactic,
        profileId,        
        countryName,
        accountName,
        accountId,
        CAST(reportDate as DATE) reportDate,
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
        case when reportDate <= '2022-01-15' then attributedSales14d 
        else viewAttributedSales14d 
        end as attributedSales14d,
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
        c.value as conversion_rate,
        c.to_currency_code as conversion_currency,
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        DENSE_RANK() OVER (PARTITION BY reportDate,CampaignId, adGroupID,
        asin, sku order by a._daton_batch_runtime desc) row_num
        from {{i}} a left join {{ref('ExchangeRates')}} c on date(a.RequestTime) = c.date and a.currency = c.to_currency_code
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            --WHERE 1=1
            WHERE a._daton_batch_runtime  >= {{max_loaded}}
            {% endif %}
    
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    cluster_by = ['campaignId','campaignStatus'], 
    partition_by = { 'field': 'reportDate', 'data_type': 'date' },
    unique_key = ['reportdate','campaignId','placement'])}}

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
where lower(table_name) like '%sponsoredbrands_placementcampaignsreport' 
{% endset %}  



{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[0] %}
    SELECT * except(row_num)
    From (
        select '{{id}}' as brand,
        CAST(RequestTime as timestamp) RequestTime,
        profileId,
        countryName,
        accountName,
        accountId,
        CAST(reportDate as DATE) reportDate,
        placement,
        campaignId,
        campaignBudgetType,
        campaignStatus,
        campaignName,
        impressions,
        clicks,
        cost,
        attributedDetailPageViewsClicks14d,
        attributedSales14d,
        attributedSales14dSameSKU,
        attributedConversions14d,
        attributedConversions14dSameSKU,
        attributedOrdersNewToBrand14d,
        attributedOrdersNewToBrandPercentage14d,
        attributedOrderRateNewToBrand14d,
        attributedSalesNewToBrand14d,
        attributedSalesNewToBrandPercentage14d,
        attributedUnitsOrderedNewToBrand14d,
        attributedUnitsOrderedNewToBrandPercentage14d,
        unitsSold14d,
        dpv14d,
        applicableBudgetRuleId,
        applicableBudgetRuleName,
        campaignRuleBasedBudget,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,

        DENSE_RANK() OVER (PARTITION BY reportdate,campaignId,placement order by _daton_batch_runtime desc) row_num
        from {{i}}    
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            --WHERE 1=1
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
{% if var('SDProductTargetingReport') %}
    {{ config( enabled = True,
    post_hook = "drop table {{this|replace('SDProductTargetingReport', 'SDProductTargetingReport_temp')}}"
    ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%sponsoreddisplay_producttargetingreport')}}    
{% endset %}  


{% set results = run_query(table_name_query) %}
{% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
    {% set results_list = [] %}
    {% set tables_lowercase_list = [] %}
{% endif %}


{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list %}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    {% if i==results_list[0] %}
        {% set action1 = 'create or replace table' %}
        {% set tbl = this ~ ' as ' %}
    {% else %}
        {% set action1 = 'insert into ' %}
        {% set tbl = this %}
    {% endif %}

    {%- set query -%}
    {{action1}}
    {{tbl|replace('SDProductTargetingReport', 'SDProductTargetingReport_temp')}}

    select 
    '{{brand}}' as brand,
    '{{store}}' as store,
    cast(RequestTime as timestamp) RequestTime,
    tactic,
    profileId,
    countryName,
    accountName,
    accountId,
    reportDate,
    coalesce(targetId,'N/A') as targetId,
    targetingExpression,
    targetingText,
    targetingType,
    campaignName,
    coalesce(campaignId,'N/A') as campaignId, 
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
    attributedSales1d,			
    attributedSales7d,			
    attributedSales14d,			
    attributedSales30d			
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
    adGroupId,
    adGroupName,			
    attributedBrandedSearches14d,		
    viewAttributedBrandedSearches14d,		
    viewAttributedOrdersNewToBrand14d,		
    viewAttributedSalesNewToBrand14d,			
    viewAttributedUnitsOrderedNewToBrand14d,
    {% if var('currency_conversion_flag') %}
        case when c.value is null then 1 else c.value end as exchange_currency_rate,
        case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
    {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        a.currency as exchange_currency_code, 
    {% endif %}
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,            
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
    {% if var('currency_conversion_flag') %}
        left join {{ref('ExchangeRates')}} c on date(a.RequestTime) = c.date and a.currency = c.to_currency_code
    {% endif %}
    {% if is_incremental() %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where a.{{daton_batch_runtime()}}  >= {{max_loaded}}
    {% endif %} 
    qualify row_number() over (partition by reportDate,campaignId,targetId,targetingType order by a.{{daton_batch_runtime()}} desc) = 1 
    {% endset %}

    {% do run_query(query) %}

{% endfor %}
select * from {{this|replace('SDProductTargetingReport', 'SDProductTargetingReport_temp')}}    

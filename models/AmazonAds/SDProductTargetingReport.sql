{% if var('SDProductTargetingReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('sd_producttargeting_tbl_ptrn'),
exclude=var('sd_producttargeting_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    select 
    '{{brand|replace("`","")}}' as brand,
    '{{store|replace("`","")}}' as store,
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
    where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sd_producttargeting_lookback') }},0) from {{ this }})
    {% endif %} 
    qualify row_number() over (partition by reportDate,campaignId,targetId,targetingType order by a.{{daton_batch_runtime()}} desc) = 1 

{% if not loop.last %} union all {% endif %}
{% endfor %}

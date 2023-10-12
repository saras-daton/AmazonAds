{% if var('SBUnifiedTargetReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('sb_unifiedtarget_tbl_ptrn'),
exclude=var('sb_unifiedtarget_tbl_exclude_ptrn'),
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
    profileId,
    countryName,
    accountName,
    accountId,
    reportDate,
    campaignId, 
    campaignName,
    adGroupId,
    adGroupName,
    campaignBudgetType,
    campaignStatus,
    targetId,
    targetingExpression,
    targetingType,
    targetingText,
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
    campaignBudget,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,            
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
    {% if is_incremental() %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sb_unifiedtarget_lookback') }},0) from {{ this }})
    {% endif %} 
    qualify row_number() over (partition by reportDate,campaignId,targetId,targetingType order by {{daton_batch_runtime()}} desc) = 1

{% if not loop.last %} union all {% endif %}
{% endfor %}   

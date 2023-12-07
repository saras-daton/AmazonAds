{% if var('SBUnifiedKeywordsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("sb_unifiedkeywords_tbl_ptrn","sb_unifiedkeywords_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    RequestTime,
    profileId,
    countryName,
    accountName,
    accountId,
    reportDate,
    campaignId,
    campaignStatus,
    campaignBudget,
    campaignBudgetType,
    campaignName,
    impressions,
    keywordBid,
    keywordId,
    keywordStatus,
    keywordText,
    matchType,
    clicks,
    cost,
    unitsSold14d,
    attributedSales14d,
    attributedSales14dSameSKU,
    attributedConversions14d,
    attributedConversions14dSameSKU,
    adGroupName,
    adGroupId,
    vctr,
    video5SecondViewRate,
    video5SecondViews,
    videoFirstQuartileViews,
    videoMidpointViews,
    videoThirdQuartileViews,
    videoUnmutes,
    viewableImpressions,
    vtr,
    dpv14d,
    attributedDetailPageViewsClicks14d,
    attributedOrderRateNewToBrand14d,
    attributedOrdersNewToBrand14d,
    attributedOrdersNewToBrandPercentage14d,
    attributedSalesNewToBrand14d,
    attributedSalesNewToBrandPercentage14d,
    attributedUnitsOrderedNewToBrand14d,
    attributedUnitsOrderedNewToBrandPercentage14d,
    attributedBrandedSearches14d,
    currency,
    topOfSearchImpressionShare,
    videoCompleteViews,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,            
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sb_unifiedkeywords_lookback') }},0) from {{ this }})
        {% endif %} 
    qualify row_number() over (partition by reportDate,campaignId,keywordId,matchType order by {{daton_batch_runtime()}} desc) = 1

{% if not loop.last %} union all {% endif %}
{% endfor %}

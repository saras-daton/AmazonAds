{% if var('SDProductAdsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("sd_productads_tbl_ptrn","sd_productads_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    RequestTime,
    tactic,
    profileId,        
    countryName,
    accountName,
    accountId,
    reportDate,
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
        {#/*Currency_conversion as exchange_rates alias can be differnt we have value and from_currency_code*/#}
        {{ currency_conversion('c.value', 'c.from_currency_code', 'currency') }},
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
            WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sd_productads_lookback') }},0) from {{ this }})
        {% endif %}    
        qualify row_number() over (partition by reportDate,CampaignId, adGroupID,asin, sku order by a.{{daton_batch_runtime()}} desc) = 1 

{% if not loop.last %} union all {% endif %}
{% endfor %}   

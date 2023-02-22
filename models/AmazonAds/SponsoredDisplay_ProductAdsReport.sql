{% if var('SponsoredDisplay_ProductAdsReport') %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
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
    {{set_table_name('%sponsoreddisplay_productadsreport')}}    
    {% endset %}  


    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
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

        SELECT * {{exclude()}} (row_num)
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(RequestTime as timestamp)") }} as {{ dbt.type_timestamp() }}) as RequestTime,
            tactic,
            profileId,        
            countryName,
            accountName,
            accountId,
            CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(reportDate as timestamp)") }} as {{ dbt.type_timestamp() }}) as reportDate,
            adGroupName,
            coalesce(adGroupId,'') as adGroupId,
            coalesce(asin,'') as asin,
            coalesce(sku,'') as sku,
            adId,
            campaignName,
            coalesce(campaignId,'') as campaignId,
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
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            ROW_NUMBER() OVER (PARTITION BY reportDate,CampaignId, adGroupID,asin, sku order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
            
                {% if var('currency_conversion_flag') %} 
                    
                    left join {{ref('ExchangeRates')}} c on date(a.RequestTime) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}    
        )
         where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}

{% endif %}
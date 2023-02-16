-- depends_on: {{ref('ExchangeRates')}}
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
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
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



    {% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
    {% endif %}

    {% for i in results_list %}
        {% if var('brand_consolidation_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brand_name_position')] %}
        {% else %}
            {% set brand = var('brand_name') %}
        {% endif %}

        {% if var('store_consolidation_flag') %}
            {% set store =i.split('.')[2].split('_')[var('store_name_position')] %}
        {% else %}
            {% set store = var('store') %}
        {% endif %}

        SELECT * {{exclude()}} (row_num)
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            {% if var('timezone_conversion_flag') %}
                cast(DATETIME_ADD(RequestTime, INTERVAL {{hr}} HOUR ) as Date) RequestTime,
            {% else %}
                cast(RequestTime as DATE) RequestTime,
            {% endif %}	
            tactic,
            profileId,        
            countryName,
            accountName,
            accountId,
            {% if var('timezone_conversion_flag') %}
                cast(DATETIME_ADD(cast(reportDate as timestamp), INTERVAL {{hr}} HOUR ) as DATE) reportDate,
            {% else %}
                cast(reportDate as DATE) reportDate,
            {% endif %}
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
                cast(null as string) as exchange_currency_code,
            {% endif %}
	        a.{{daton_user_id()}},
            a.{{daton_batch_runtime()}},
            a.{{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(reportdate as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id,
            {% else %}
                cast(reportdate as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id,
            {% endif %}
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

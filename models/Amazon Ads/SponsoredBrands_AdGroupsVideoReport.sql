
{% if var('SponsoredBrands_AdGroupsVideoReport') %}

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
    {{set_table_name('%sponsoredbrands_adgroupsvideoreport')}}    
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

        SELECT * {{exclude()}} (row_num)
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            CAST(RequestTime as date) as RequestTime,
            profileId,
            countryName,
            accountName,
            accountId,
            {% if var('timezone_conversion_flag') %}
                cast(DATETIME_ADD(cast(reportDate as timestamp), INTERVAL {{hr}} HOUR ) as DATE) reportDate,
            {% else %}
                cast(reportDate as DATE) reportDate,
            {% endif %}
            coalesce(campaignId,'') as campaignId,
            campaignName,
            campaignBudget,
            campaignBudgetType,
            campaignStatus,
            coalesce(adGroupId,'') as adGroupId,
            adGroupName,
            impressions,
            clicks,
            cost,
            attributedSales14d,
            attributedSales14dSameSKU,
            attributedConversions14d,
            attributedConversions14dSameSKU,
            attributedDetailPageViewsClicks14d,
            attributedOrderRateNewToBrand14d,
            attributedOrdersNewToBrand14d,
            attributedOrdersNewToBrandPercentage14d,
            attributedSalesNewToBrand14d,
            attributedSalesNewToBrandPercentage14d,
            attributedUnitsOrderedNewToBrand14d,
            attributedUnitsOrderedNewToBrandPercentage14d,
            dpv14d,
            vctr,
            video5SecondViewRate,
            video5SecondViews,
            videoCompleteViews,
            videoFirstQuartileViews,
            videoMidpointViews,
            videoThirdQuartileViews,
            videoUnmutes,
            viewableImpressions,
            vtr,
            CAST(0 as int) units_sold,
	        {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,            
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            DENSE_RANK() OVER (PARTITION BY reportDate, campaignId, adGroupId order by {{daton_batch_runtime()}} desc) row_num
            from {{i}} 
                        {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}  
            )
         where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
{% endif %}
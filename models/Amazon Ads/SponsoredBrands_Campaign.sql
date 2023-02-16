
    {% if var('table_partition_flag') %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        partition_by = { 'field': 'fetchDate', 'data_type': 'date' },
        cluster_by = ['fetchDate', 'campaignId'], 
        unique_key = ['fetchDate', 'campaignId'])}}
    {% else %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['fetchDate', 'campaignId'])}}
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
    {{set_table_name('%sponsoredbrands_campaign')}}    
    {% endset %}  


    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
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
            '{{brand}}' as Brand,
            '{{store}}' as store,
            cast(RequestTime as timestamp) RequestTime,
            profileId,
            countryName,
            accountName,
            accountId,
            CAST(fetchDate as Date) fetchDate,
            campaignId,
            name,
            budget,
            budgetType,
            startDate,
            endDate,
            state,
            servingStatus,
            bidOptimization,
            bidMultiplier,
            portfolioId,
            {% if var('snowflake_database_flag') %}
            CREATIVE.VALUE:brandName :: VARCHAR as brandname,
            CREATIVE.VALUE:brandLogoAssetID :: VARCHAR as brandLogoAssetID ,
            CREATIVE.VALUE:brandLogoUrl :: VARCHAR as brandLogoUrl,
            CREATIVE.VALUE:headline :: VARCHAR as headline,
            CREATIVE.VALUE:asins :: VARCHAR as asins,
            CREATIVE.VALUE:shouldOptimizeAsins :: VARCHAR as shouldOptimizeAsins,
            LANDINGPAGE.VALUE:url :: VARCHAR as landingPageURL,
            brandEntityId,
            BIDADJUSTMENTS.VALUE:bidAdjustmentPredicate :: VARCHAR as bidAdjustmentPredicate,
            BIDADJUSTMENTS.VALUE:bidAdjustmentPercent :: FLOAT as bidAdjustmentPercent,
            {% else %}
            CREATIVE.brandName,
            CREATIVE.brandLogoAssetID,
            CREATIVE.brandLogoUrl,
            CREATIVE.headline,
            CREATIVE.asins,
            CREATIVE.shouldOptimizeAsins,
            LANDINGPAGE.url,
            brandEntityId,
            BIDADJUSTMENTS.bidAdjustmentPredicate,
            BIDADJUSTMENTS.bidAdjustmentPercent,
            {% endif %}
            adFormat,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(RequestTime as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id,
            {% else %}
                cast(RequestTime as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id,
            {% endif %}
            DENSE_RANK() OVER (PARTITION BY campaignId, fetchDate order by {{daton_batch_runtime()}} desc) row_num
            FROM {{i}}
            {{unnesting("CREATIVE")}} 
            {{unnesting("LANDINGPAGE")}} 
            {{unnesting("BIDADJUSTMENTS")}} 
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
            )
            where row_num = 1
            {% if not loop.last %} union all {% endif %}
     {% endfor %}


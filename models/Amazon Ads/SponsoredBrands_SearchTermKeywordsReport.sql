
    {% if var('table_partition_flag') %}
    {{config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        partition_by = { 'field': 'reportDate', 'data_type': 'date' },
        cluster_by = ['campaignId','keywordId','matchType'], 
        unique_key = ['reportDate','campaignId','keywordId','matchType','query'])}}
    {% else %}
    {{config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        unique_key = ['reportDate','campaignId','keywordId','matchType','query'])}}
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
    {{set_table_name('%sponsoredbrands_searchtermkeywordsreport')}}    
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
            CAST(RequestTime as timestamp) RequestTime,
            impressions,
            clicks,
            cost,
            attributedConversions14d,
            attributedSales14d,
            profileId,
            countryName,
            accountName,
            accountId,
            {% if var('timezone_conversion_flag') %}
                cast(DATETIME_ADD(cast(reportDate as timestamp), INTERVAL {{hr}} HOUR ) as DATE) reportDate,
            {% else %}
                cast(reportDate as DATE) reportDate,
            {% endif %}
            coalesce(query,'') as query,
            coalesce(campaignId,'') as campaignId, 
            campaignName,
            adGroupId,
            adGroupName,
            campaignBudgetType,
            campaignStatus,
            coalesce(keywordId,'') as keywordId,
            keywordStatus,
            KeywordBid,
            keywordText,
            coalesce(matchType,'') as matchType,
            campaignBudget,
            searchTermImpressionRank,
            searchTermImpressionShare,
            CAST(null as int) units_sold,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
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
            ROW_NUMBER() OVER (PARTITION BY reportDate,campaignId,keywordId,matchType,
            query order by {{daton_batch_runtime()}} desc) row_num
            from {{i}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %} 
        )
        where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}


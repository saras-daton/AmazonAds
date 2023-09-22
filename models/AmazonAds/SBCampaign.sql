{% if var('SBCampaign') %}
    {{ config( enabled = True,
    post_hook = "drop table {{this|replace('SBCampaign', 'SBCampaign_temp')}}"
    ) }}
{% else %}
    {{ config( enabled = False ) }}
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

        {% if i==results_list[0] %}
            {% set action1 = 'create or replace table' %}
            {% set tbl = this ~ ' as ' %}
        {% else %}
            {% set action1 = 'insert into ' %}
            {% set tbl = this %}
        {% endif %}

        {%- set query -%}
        {{action1}}
        {{tbl|replace('SBCampaign', 'SBCampaign_temp')}}

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast(RequestTime as timestamp) RequestTime,
        profileId,
        countryName,
        accountName,
        accountId,
        cast(fetchDate as Date) fetchDate,
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
        {{extract_nested_value("creative","brandName","string")}} as creative_brandname,
        {{extract_nested_value("creative","brandLogoAssetID","string")}} as creative_brandLogoAssetID,
        {{extract_nested_value("creative","brandLogoUrl","string")}} as creative_brandLogoUrl,
        {{extract_nested_value("creative","headline","string")}} as creative_headline,
        {{extract_nested_value("creative","asins","string")}} as creative_asins,
        {{extract_nested_value("creative","shouldOptimizeAsins","string")}} as creative_shouldOptimizeAsins,
        {{extract_nested_value("landingpage","url","string")}} as landingPage_url,
        brandEntityId,
        {{extract_nested_value("bidAdjustments","bidAdjustmentPredicate","string")}} as bidAdjustments_bidAdjustmentPredicate,
        {{extract_nested_value("bidAdjustments","bidAdjustmentPercent","numeric")}} as bidAdjustments_bidAdjustmentPercent,
        adFormat,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}
            {{unnesting("creative")}} 
            {{unnesting("landingpage")}} 
            {{unnesting("bidAdjustments")}} 
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        qualify dense_rank() over (partition by campaignId, fetchDate, {{extract_nested_value("bidAdjustments","bidAdjustmentPredicate","string")}} order by {{daton_batch_runtime()}} desc) = 1
    {% endset %}

    {% do run_query(query) %}

    {% endfor %}
    select * from {{this|replace('SBCampaign', 'SBCampaign_temp')}}    

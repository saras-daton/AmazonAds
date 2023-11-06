{% if var('SBCampaign') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('sb_campaign_tbl_ptrn'),
exclude=var('sb_campaign_tbl_exclude_ptrn'),
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
    RequestTime,
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
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sb_campaign_lookback') }},0) from {{ this }})
        {% endif %}
    qualify dense_rank() over (partition by campaignId, fetchDate, {{extract_nested_value("bidAdjustments","bidAdjustmentPredicate","string")}} order by {{daton_batch_runtime()}} desc) = 1

{% if not loop.last %} union all {% endif %}
{% endfor %} 

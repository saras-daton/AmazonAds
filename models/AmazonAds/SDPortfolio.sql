{% if var('SDPortfolio') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("sd_portfolio_tbl_ptrn","sd_portfolio_tbl_exclude_ptrn") %}
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
    cast(fetchDate as Date) fetchDate,
    portfolioId,
    name,
    {{extract_nested_value("budget","amount","numeric")}} as budget_amount,
    {{extract_nested_value("budget","currencyCode","string")}} as budget_currencyCode,
    {{extract_nested_value("budget","policy","string")}} as budget_policy,
    {{extract_nested_value("budget","startDate","date")}} as budget_startDate,
    {{extract_nested_value("budget","endDate","date")}} as budget_endDate,
    inBudget,
    state,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,            
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} 
    {{unnesting("budget")}}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sd_portfolio_lookback') }},0) from {{ this }})
    {% endif %}
    qualify dense_rank() over (partition by fetchDate, profileId, portfolioId order by _daton_batch_runtime desc) = 1

{% if not loop.last %} union all {% endif %}
{% endfor %}

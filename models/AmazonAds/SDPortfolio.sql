{% if var('SDPortfolio') %}
    {{ config( enabled = True,
    post_hook = "drop table {{this|replace('SDPortfolio', 'SDPortfolio_temp')}}"
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
    {{set_table_name('%_sd_%portfolio')}} or lower(table_name) like '%_sponsoreddisplay_%portfolio'
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
        {{tbl|replace('SDPortfolio', 'SDPortfolio_temp')}}

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast(RequestTime as timestamp) RequestTime,
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
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
        {% endif %}
        qualify dense_rank() over (partition by fetchDate, profileId, portfolioId order by _daton_batch_runtime desc) = 1
    {% endset %}

    {% do run_query(query) %}

    {% endfor %}
    select * from {{this|replace('SDPortfolio', 'SDPortfolio_temp')}}    

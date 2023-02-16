
    {% if var('table_partition_flag') %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        partition_by = { 'field': 'fetchDate', 'data_type': 'date' },
        cluster_by = ['profileId', 'portfolioId'], 
        unique_key = ['fetchDate', 'profileId', 'portfolioId'])}}
    {% else %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['fetchDate', 'profileId', 'portfolioId'])}}
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


    with final as (
    with unnested_BUDGET as (
    {% set table_name_query %}
    {{set_table_name('%sponsoredproducts%portfolio')}}    
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

        SELECT *
            From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            cast(RequestTime as timestamp) RequestTime,
            profileId,
            countryName,
            accountName,
            accountId,
            CAST(fetchDate as Date) fetchDate,
            portfolioId,
            name,
            {% if var('snowflake_database_flag') %} 
            BUDGET.VALUE:amount :: FLOAT as amount,
            BUDGET.VALUE:currencyCode :: VARCHAR as currencyCode,
            BUDGET.VALUE:policy :: VARCHAR as policy,
            BUDGET.VALUE:startDate :: DATE as BudgetStartDate,
            BUDGET.VALUE:endDate :: DATE as BudgetEndDate,
            {% else %}
            BUDGET.amount,
            BUDGET.currencyCode,
            BUDGET.policy,
            BUDGET.startDate,
            BUDGET.endDate,
            {% endif %}
            inBudget,
            state,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(fetchDate as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id
            {% else %}
                cast(fetchDate as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id
            {% endif %}
            FROM  {{i}}  
             {{unnesting("BUDGET")}} 
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
            {% if not loop.last %} union all {% endif %}
     {% endfor %}

    ))

    select *,
    DENSE_RANK() OVER (PARTITION BY fetchDate, profileId, portfolioId order by {{daton_batch_runtime()}} desc) row_num
    FROM unnested_BUDGET
    )

    select * {{exclude()}} (row_num)
    from final
    where row_num = 1


--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('SponsoredDisplay_Portfolio', True)) }}

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
    unique_key = ['RequestTime', 'campaignId'])}}
{% endif %}


{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT MAX(_daton_batch_runtime) - 2592000000 FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

with unnested_table as(
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES 
where table_name like '%SD%Portfolio' 
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
    {% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
    {% endif %}
    SELECT * 
    FROM (
        select 
        '{{brand}}' as Brand,
        {% if var('timezone_conversion_flag') %}
            cast(DATETIME_ADD(timestamp(RequestTime), INTERVAL {{hr}} HOUR ) as timestamp) RequestTime,
        {% else %}
            cast(RequestTime as timestamp) RequestTime,
        {% endif %}
        profileId,
        countryName,
        accountName,
        accountId,
        fetchDate,
        portfolioId,
        name,
        budget.amount,
        budget.currencyCode,
        budget.policy,
        budget.startDate,
        budget.endDate,
        inBudget,
        state,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
            cast(DATETIME_ADD(timestamp(RequestTime), INTERVAL {{hr}} HOUR ) as timestamp) _edm_eff_strt_ts,
        {% else %}
            CAST(RequestTime as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        FROM  {{i}} 
                cross join unnest(budget) budget
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE _daton_batch_runtime  >= {{max_loaded}}
                {% endif %}
        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY fetchDate, profileId, portfolioId order by _daton_batch_runtime desc) row_num
from unnested_table 
)

select * except(row_num)
from dedup 
where row_num = 1
{% if var('SDProductMetadata') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("sd_productmetadata_tbl_ptrn","sd_productmetadata_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    {{ currency_conversion('c.value', 'c.from_currency_code', 'basisPrice_currency') }},
    b.* from (
    select
    RequestTime,			
    profileId,
    countryName,
    accountName,
    accountId,
    fetchDate,
    eligibilityStatus,
    {{extract_nested_value("basisPrice","amount","numeric")}} as basisPrice_amount,
    {{extract_nested_value("basisPrice","currency","string")}} as basisPrice_currency,
    case 
        when createdDate='' then Null
        else
            {% if target.type == 'snowflake'%}
                TO_DATE(createdDate, 'Mon DD, YYYY')
            {% else %}  
                PARSE_DATE('%b %d, %Y', createdDate) 
            {% endif %}    
    end as createdDate,
    imageUrl,
    {{extract_nested_value("priceToPay","amount","numeric")}} as priceToPay_amount,
    {{extract_nested_value("priceToPay","currency","string")}} as priceToPay_currency,
    asin,
    availability,
    sku,
    title,
    variationList,
    ineligibilityReasons,
    ineligibilityCodes,
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,            
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
    {{unnesting("basisPrice")}}
    {{unnesting("priceToPay")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('sd_productmetadata_lookback') }},0) from {{ this }})
        {% endif %}    
        qualify row_number() over (partition by profileId,countryName,accountName,accountId,fetchDate,asin,sku order by a.{{daton_batch_runtime()}} desc) = 1
    )b
        {% if var('currency_conversion_flag') %} 
            left join {{ref('ExchangeRates')}} c on date(b.RequestTime) = c.date and priceToPay_currency = c.to_currency_code
        {% endif %} 
{% if not loop.last %} union all {% endif %}
{% endfor %}   

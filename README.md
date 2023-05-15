# Amazon Advertising Data Unification

This dbt package is for Data Unification of Amazon Advertising ingested data by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery
- Snowflake

#### Typical challenges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Amazon
- Separate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

Data Unification simplifies Data Analytics by doing:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are Standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are Standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Amazon Ads: Sponsored Brands, Sponsored Display, Sponsored Products 
- Exchange Rates(Optional, if currency conversion is not required)

*Note:* 
*Please select 'Do Not Unnest' option while setting up Daton Integrataion*

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_amazon). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  amazon_ads:
    AmazonAds:
      +schema: custom_schema_extension

```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the timezone columns from UTC timezone to local timezone, please mark the timezone_conversion_flag as True in the dbt_project.yml file, by default, it is False.
Additionally, you need to provide offset hours between UTC and the timezone you want the data to convert into for each raw table for which you want timezone converison to be taken into account.

Example:
```yaml
vars:
timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "Amazon.Ads.SPONSOREDBRANDS_ADGROUPSREPORT":-7,
    "Amazon.Ads.SPONSOREDBRANDS_PLACEMENTCAMPAIGNSREPORT":-7,
    }
```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
SponsoredBrands_AdGroupsReport: False
```

## Models

This package contains models from the Amazon Advertising API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Sponsored Brands | [SBPortfolio](models/AmazonAds/SBPortfolio.sql)  | A list of portfolios associated with the account |
|Sponsored Brands | [SBCampaign](models/AmazonAds/SBCampaign.sql)  | A list of campaigns associated with the account |
|Sponsored Brands | [SBAdGroupsReport](models/AmazonAds/SBAdGroupsReport.sql)  | A list of ad groups associated with the account |
|Sponsored Brands | [SBAdGroupsVideoReport](models/AmazonAds/SBAdGroupsVideoReport.sql)| A list of ad groups related to sponsored brand video associated with the account |
|Sponsored Brands | [SBPlacementCampaignsReport](models/AmazonAds/SBPlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Brands | [SBSearchTermKeywordsReport](models/AmazonAds/SBSearchTermKeywordsReport.sql)| A list of product search keywords report |
|Sponsored Brands | [SBSearchTermKeywordsVideoReport](models/AmazonAds/SBSearchTermKeywordsVideoReport.sql)| A list of keywords associated with sponsored brand video |
|Sponsored Display | [SDPortfolio](models/AmazonAds/SDPortfolio.sql)| A list of portfolios associated with the account |
|Sponsored Display | [SDCampaign](models/AmazonAds/SDCampaign.sql)| A list of campaigns associated with the account |
|Sponsored Display | [SDProductAdsReport](models/AmazonAds/SDProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SPPortfolio](models/AmazonAds/SPPortfolio.sql)| A list of portfolios associated with the account |
|Sponsored Products | [SPCampaign](models/AmazonAds/SPCampaign.sql)| A list of campaigns associated with the account |
|Sponsored Products | [SPPlacementCampaignsReport](models/AmazonAds/SPPlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Products | [SPProductAdsReport](models/AmazonAds/SPProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SPSearchTermKeywordReport](models/AmazonAds/SPSearchTermKeywordReport.sql)| A list of product search keywords report |




### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: SBPortfolio
    description: A list of portfolios associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['profileId', 'portfolioId'] 
      unique_key: ['fetchDate', 'profileId', 'portfolioId']
    
  - name: SBCampaign	
    description: A list of campaigns associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['fetchDate', 'campaignId'] 
      unique_key: ['fetchDate', 'campaignId']

  - name: SBAdGroupsReport
    description: A list of ad groups associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','adGroupId'] 
      unique_key: ['reportDate','campaignId','adGroupId']

  - name: SBAdGroupsVideoReport
    description: A list of ad groups related to sponsored brand video associated with the account
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','adGroupId'] 
      unique_key: ['reportDate','campaignId','adGroupId']

  - name: SBPlacementCampaignsReport
    description: A list of all the placement campaigns associated with the account.
    config:
      materialized: incremental 
      incremental_strategy: merge
      cluster_by: ['campaignId','campaignStatus'] 
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      unique_key: ['reportdate','campaignId','placement']

  - name: SBSearchTermKeywordsReport
    description: A list of product search keywords report
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','keywordId','matchType'] 
      unique_key: ['reportDate','campaignId','keywordId','matchType','query']

  - name: SBSearchTermKeywordsVideoReport
    description: A list of keywords associated with sponsored brand video
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','keywordId','matchType'] 
      unique_key: ['reportDate','campaignId','keywordId','matchType','query']

  - name: SDPortfolio
    description: A list of portfolios associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['profileId', 'portfolioId'] 
      unique_key: ['fetchDate', 'profileId', 'portfolioId']

  - name: SDCampaign
    description: A list of campaigns associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['fetchDate', 'campaignId'] 
      unique_key: ['fetchDate', 'campaignId']

  - name: SDProductAdsReport
    description: A list of product ads associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['CampaignId', 'adGroupID', 'asin', 'sku'] 
      unique_key: ['reportDate','CampaignId', 'adGroupID', 'asin', 'sku']

  - name: SPPortfolio
    description: A list of portfolios associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['profileId', 'portfolioId'] 
      unique_key: ['fetchDate', 'profileId', 'portfolioId']

  - name: SPCampaign
    description: A list of campaigns associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['fetchDate', 'campaignId'] 
      unique_key: ['fetchDate', 'campaignId']

  - name: SPPlacementCampaignsReport
    description: A list of all the placement campaigns associated with the account
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','placement'] 
      unique_key: ['reportDate','campaignId','placement']

  - name: SPProductAdsReport
    description: A list of product ads associated with the account
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId', 'adGroupId','asin','sku'] 
      unique_key: ['reportDate', 'campaignId', 'adGroupId','adId']

  - name: SPSearchTermKeywordReport
    description: A list of product search keywords report
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','adGroupId','keywordId','matchType'] 
      unique_key: ['reportDate','campaignId','adGroupId','keywordId','matchType','query','impressions']
   
```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}

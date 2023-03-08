# Amazon Advertising Data Unification

his dbt package is for Data Unification of Amazon Advertising ingested data by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

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

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your DBT project. Include this in your `packages.yml` file

```yaml
packages:
  - package: saras-daton/amazon_ads
    version: {{1.0.1}}
```

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

To enable timezone conversion, which converts the datetime columns from local timezone to given timezone, please mark the timezone_conversion_flag f as True in the dbt_project.yml file, by default, it is False
Additionally, you need to provide offset hours for each raw table

Example:
```yaml
vars:
timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "Amazon.Ads.SPONSOREDBRANDS_ADGROUPSREPORT":-4,
    "Amazon.Ads.SPONSOREDBRANDS_PLACEMENTCAMPAIGNSREPORT":-4,
    "Amazon.Ads.SPONSOREDBRANDS_SEARCHTERMKEYWORDSREPORT":-4,
    "Amazon.Ads.SPONSOREDPRODUCTS_PLACEMENTCAMPAIGNSREPORT":-4,
    "Amazon.Ads.SPONSOREDPRODUCTS_PRODUCTADSREPORT":-4
    }
```

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
|Sponsored Brands | [SB_Portfolio](models/AmazonAds/SB_Portfolio.sql)  | A list of portfolios associated with the account |
|Sponsored Brands | [SB_Campaign](models/AmazonAds/SB_Campaign.sql)  | A list of campaigns associated with the account |
|Sponsored Brands | [SB_AdGroupsReport](models/AmazonAds/SB_AdGroupsReport.sql)  | A list of ad groups associated with the account |
|Sponsored Brands | [SB_AdGroupsVideoReport](models/AmazonAds/SB_AdGroupsVideoReport.sql)| A list of ad groups related to sponsored brand video associated with the account |
|Sponsored Brands | [SB_PlacementCampaignsReport](models/AmazonAds/SB_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Brands | [SB_SearchTermKeywordsReport](models/AmazonAds/SB_SearchTermKeywordsReport.sql)| A list of product search keywords report |
|Sponsored Brands | [SB_SearchTermKeywordsVideoReport](models/AmazonAds/SB_SearchTermKeywordsVideoReport.sql)| A list of keywords associated with sponsored brand video |
|Sponsored Display | [SD_Portfolio](models/AmazonAds/SD_Portfolio.sql)| A list of portfolios associated with the account |
|Sponsored Display | [SD_Campaign](models/AmazonAds/SD_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Display | [SD_ProductAdsReport](models/AmazonAds/SD_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SP_Portfolio](models/AmazonAds/SP_Portfolio.sql)| A list of portfolios associated with the account |
|Sponsored Products | [SP_Campaign](models/AmazonAds/SP_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Products | [SP_PlacementCampaignsReport](models/AmazonAds/SP_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Products | [SP_ProductAdsReport](models/AmazonAds/SP_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SP_SearchTermKeywordReport](models/AmazonAds/SP_SearchTermKeywordReport.sql)| A list of product search keywords report |




### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: SB_Portfolio
    description: A list of portfolios associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['profileId', 'portfolioId'] 
      unique_key: ['fetchDate', 'profileId', 'portfolioId']
    
  - name: SB_Campaign	
    description: A list of campaigns associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['fetchDate', 'campaignId'] 
      unique_key: ['fetchDate', 'campaignId']

  - name: SB_AdGroupsReport
    description: A list of ad groups associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','adGroupId'] 
      unique_key: ['reportDate','campaignId','adGroupId']

  - name: SB_AdGroupsVideoReport
    description: A list of ad groups related to sponsored brand video associated with the account
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','adGroupId'] 
      unique_key: ['reportDate','campaignId','adGroupId']

  - name: SB_PlacementCampaignsReport
    description: A list of all the placement campaigns associated with the account.
    config:
      materialized: incremental 
      incremental_strategy: merge
      cluster_by: ['campaignId','campaignStatus'] 
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      unique_key: ['reportdate','campaignId','placement']

  - name: SB_SearchTermKeywordsReport
    description: A list of product search keywords report
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','keywordId','matchType'] 
      unique_key: ['reportDate','campaignId','keywordId','matchType','query']

  - name: SB_SearchTermKeywordsVideoReport
    description: A list of keywords associated with sponsored brand video
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','keywordId','matchType'] 
      unique_key: ['reportDate','campaignId','keywordId','matchType','query']

  - name: SD_Portfolio
    description: A list of portfolios associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['profileId', 'portfolioId'] 
      unique_key: ['fetchDate', 'profileId', 'portfolioId']

  - name: SD_Campaign
    description: A list of campaigns associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['fetchDate', 'campaignId'] 
      unique_key: ['fetchDate', 'campaignId']

  - name: SD_ProductAdsReport
    description: A list of product ads associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['CampaignId', 'adGroupID', 'asin', 'sku'] 
      unique_key: ['reportDate','CampaignId', 'adGroupID', 'asin', 'sku']

  - name: SP_Portfolio
    description: A list of portfolios associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['profileId', 'portfolioId'] 
      unique_key: ['fetchDate', 'profileId', 'portfolioId']

  - name: SP_Campaign
    description: A list of campaigns associated with the account
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'fetchDate', 'data_type': date }
      cluster_by: ['fetchDate', 'campaignId'] 
      unique_key: ['fetchDate', 'campaignId']

  - name: SP_PlacementCampaignsReport
    description: A list of all the placement campaigns associated with the account
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId','placement'] 
      unique_key: ['reportDate','campaignId','placement']

  - name: SP_ProductAdsReport
    description: A list of product ads associated with the account
    config: 
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'reportDate', 'data_type': timestamp }
      cluster_by: ['campaignId', 'adGroupId','asin','sku'] 
      unique_key: ['reportDate', 'campaignId', 'adGroupId','adId']

  - name: SP_SearchTermKeywordReport
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
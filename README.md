# Amazon Ads Data Modelling
This DBT package models the Amazon Ads data coming from [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

This package would be performing the following funtions:

- Consolidation - Different marketplaces & different brands would have similar tables. Helps in consolidating all the tables into one final stage table 
- Deduplication - Based on primary keys , the tables are deduplicated and the latest records are only loaded into the stage models
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- (Optional) Currency Conversion - Based on the currency input, a couple of currency columns are generated to aid in the currency conversion - (Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this]())
- (Optional) Time Zone Conversion - Based on the time zone input, the relevant datetime columns are replaced with the converted values

#### Prerequisite 
Daton Connectors for Amazon Ads Data - Amazon Sponsored Brands, Amazon Sponsored Display, Amazon Sponsored Products, Exchange Rates(Optional)

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your project. Include this in your `packages.yml` file

```yaml
packages:
  - package: saras-daton/amazon_ads_bigquery
    version: 0.1.0
```

# Configuration 

## Required Variables

This package assumes that you have an existing DBT project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.

```yaml
vars:
    raw_projectid: "your_gcp_project"
    raw_dataset: "your_amazon_ads_dataset"
```

## Schema Change

We will create the models under the schema (<target_schema>_stg_amazon). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yml
models:
  amazon_ads_bigquery:
    +schema: custom_schema_name # leave blank for just the target_schema
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file under the above variables. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

```yaml
vars:
    currency_conversion_flag: False
    timezone_conversion_flag:
        amazon_ads: False
    timezone_conversion_hours: 7
    table_partition_flag: False
    SponsoredBrands_Portfolio: True
    brand_consolidation_flag: False
    brand_name_position: 0
    brand_name: "ABC"
```

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate, exchange_currency_code  based on the data from the Exchange Rates Connector from Daton.  please mark the currency conversion flag as True. By default, it is False.

### Timezone Conversion 

To enable timezone conversion, which converts the major date columns according to given timezone, please mark the time zone conversion variable for this pakage as True in the dbt_project.yml file. The Ads data is available at local timezone and by setting the timezone_conversion_hours variable, it will be offset by the specified number of hours.(Eg: 7,8,-7,-11 etc) By default, it is False.

### Table Partitions

To enable partitioning for the tables, please mark table_partition_flag variable as True. By default, it is False.

### Table Exclusions

Setting these table exclusions will remove the modelling enabled for the below models. By declaring the model names as variables as above and marking them as False, they get disabled. Refer the table below for model details. By default, all tables are created. 

### Brand Consolidation

The Amazon Seller Name would be called the Brand name in this case. If you sell more than one brand, then brand consolidation can be enabled. Brand name positions (Eg: 0/1/2) gets the brand name from the integration name based on the location you have given. In case there is only a single brand, adding the name in brand name variable adds the column to the tables. By default, brand consolidation flag is False and brand name variable needs to be set.

## Scheduling the Package for refresh

The ad tables that are being generated as part of this package are enabled for incremental refresh and can be scheduled by creating the job in Production Environment by giving the below command.

```
dbt run --select amazon_ads_bigquery
```

## Models

This package contains models from the Amazon API which includes Sponsored Brands, Products, Display. Please follow this to get more details about [models](https://docs.google.com/spreadsheets/d/1OaJnVpBrPZaBusJXBHrT8dhnD2zctWMmSRN__WLQsl0/edit?usp=sharing). The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Sponsored Brands | [SB_Portfolio](models/Amazon%20Ads/Sponsored%20Brands/SB_Portfolio.sql)  | A list of portfolios associated with the account |
|Sponsored Brands | [SB_Campaign](models/Amazon%20Ads/Sponsored%20Brands/SB_Campaign.sql)  | A list of campaigns associated with the account |
|Sponsored Brands | [SB_AdGroupsReport](models/Amazon%20Ads/Sponsored%20Brands/SB_AdGroupsReport.sql)  | A list of ad groups associated with the account |
|Sponsored Brands | [SB_AdGroupsVideoReport](models/Amazon%20Ads/Sponsored%20Brands/SB_AdGroupsVideoReport.sql)| A list of ad groups related to sponsored brand video associated with the account |
|Sponsored Brands | [SB_PlacementCampaignsReport](models/Amazon%20Ads/Sponsored%20Brands/SB_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Brands | [SB_SearchTermKeywordsReport](models/Amazon%20Ads/Sponsored%20Brands/SB_SearchTermKeywordsReport.sql)| A list of product search keywords report |
|Sponsored Brands | [SB_SearchTermKeywordsVideoReport](models/Amazon%20Ads/Sponsored%20Brands/SB_SearchTermKeywordsVideoReport.sql)| A list of keywords associated with sponsored brand video |
|Sponsored Display | [SD_Portfolio](models/Amazon%20Ads/Sponsored%20Display/SD_Portfolio.sql)| A list of portfolios associated with the account |
|Sponsored Display | [SD_Campaign](models/Amazon%20Ads/Sponsored%20Display/SD_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Display | [SD_ProductAdsReport](models/Amazon%20Ads/Sponsored%20Display/SD_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SP_Portfolio](models/Amazon%20Ads/Sponsored%20Products/SP_Portfolio.sql)| A list of portfolios associated with the account |
|Sponsored Products | [SP_Campaign](models/Amazon%20Ads/Sponsored%20Products/SP_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Products | [SP_PlacementCampaignsReport](models/Amazon%20Ads/Sponsored%20Products/SP_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Products | [SP_ProductAdsReport](models/Amazon%20Ads/Sponsored%20Products/SP_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SP_SearchTermKeywordReport](models/Amazon%20Ads/Sponsored%20Products/SP_SearchTermKeywordReport.sql)| A list of product search keywords report |

## Resources:
- Have questions, feedback, or need [help](https://calendly.com/priyanka-vankadaru/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a DBT account & connect to Bigquery

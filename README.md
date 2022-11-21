# Amazon Ads Data Modelling
This DBT package models the Amazon Advertising data coming from [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

Daton Connectors for Amazon Ads Data - Amazon Sponsored Brands, Amazon Sponsored Display, Amazon Sponsored Products

This package would be performing the following funtions:

- Consolidation - Different marketplaces & different brands would have similar tables. Helps consolidated all the tables into one final stage table 
- Deduplication - Based on primary keys , the tables are deduplicated and the latest records are only loaded into the stage table
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- (Optional) Currency Conversion - Based on the currency input, a couple of currency columns are generated to aid in the currency conversion - (Prequiste - Exchange Rates connector in Daton needs to be present - Refer [this]())
- (Optional) Time Zone Conversion - Based on the time zone input, a timezone column with the converted timestamp is created

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your project. Include this in your `packages.yml` file

```yaml
packages:
  - package: daton/amazon_advertising_bigquery
    version: [">=0.1.0", "<0.3.0"]
```

# Configuration 

## Required Variables

This package assumes that you have an existing DBT project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.

```
vars:
    raw_projectid: "your_gcp_project"
    raw_dataset: "your_amazon_advertising_dataset"
```

## Optional Variables

### Currency Conversion 

To enable currency conversion, which produces two columns - conversion_rate, conversion_currency based on the data from the Exchange Rates Connector from Daton.  please add the following in the dbt_project.yml file. By default, it is False.

```
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the major date columns according to given timezone,.  please add the following in the dbt_project.yml file. The data is available at UTC timezone and by setting the hr variable, it will be offset by the specified number of hours.(Eg: 7,8,-7,-11 etc) By default, it is False.

```
vars:
    timezone_conversion_flag: False
    timezone_conversion_hours: 7
```

### Table Partitions

To enable partitioning for the tables, please add the following in the dbt_project.yml file. By default, it is False.

```
vars:
    table_partition_flag: False
```

### Table Exclusions

Setting these table exclusions will remove the modelling enabled for the below tables. By declaring the model names as variables as below, they get disabled. Refer the table below for model details. By default, these tables are tagged True. 

```
vars:
    SponsoredBrands_Portfolio: True
```

## Scheduling the Package for refresh

The ad tables that are being generated as part of this package are enabled for incremental refresh and can be scheduled by creating the job in Production Environment by giving the below command.

```
dbt run --select amazon_advertising_bigquery
```

## Models

This package contains models from the Amazon API which includes Sponsored Brands, Products, Display. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Sponsored Brands | [SponsoredBrands_Portfolio](models/Amazon%20Adverstising/Sponsored%20Brands/SponsoredBrands_Portfolio.sql)  | A list of portfolios associated with the account |
|Sponsored Brands | [SponsoredBrands_Campaign](models/Amazon%20Adverstising/Sponsored%20Brands/SponsoredBrands_Campaign.sql)  | A list of campaigns associated with the account |
|Sponsored Brands | [SponsoredBrands_AdGroupsReport](models/Amazon%20Adverstising/Sponsored%20Brands/SponsoredBrands_AdGroupsReport.sql)  | A list of ad groups associated with the account |
|Sponsored Brands | [SponsoredBrands_AdGroupsVideoReport](models/Amazon%20Adverstising/Sponsored%20Brands/SponsoredBrands_AdGroupsVideoReport.sql)| A list of ad groups related to sponsored brand video associated with the account |
|Sponsored Brands | [SponsoredBrands_PlacementCampaignsReport](models/Amazon%20Adverstising/Sponsored%20Brands/SponsoredBrands_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Brands | [SponsoredBrands_SearchTermKeywordsReport](models/Amazon%20Adverstising/Sponsored%20Brands/SponsoredBrands_SearchTermKeywordsReport.sql)| A list of product search keywords report |
|Sponsored Brands | [SponsoredBrands_SearchTermKeywordsVideoReport](models/Amazon%20Adverstising/Sponsored%20Brands/SponsoredBrands_SearchTermKeywordsVideoReport.sql)| A list of keywords associated with sponsored brand video |
|Sponsored Display | [SponsoredDisplay_Portfolio](models/Amazon%20Adverstising/Sponsored%20Display/SponsoredDisplay_ProductAdsReport.sql)| A list of portfolios associated with the account |
|Sponsored Display | [SponsoredDisplay_Campaign](models/Amazon%20Adverstising/Sponsored%20Display/SponsoredBrands_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Display | [SponsoredDisplay_ProductAdsReport](models/Amazon%20Adverstising/Sponsored%20Display/SponsoredDisplay_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SponsoredProducts_Portfolio](models/Amazon%20Adverstising/Sponsored%20Products/SponsoredProducts_Portfolio.sql)| A list of portfolios associated with the account |
|Sponsored Products | [SponsoredProducts_Campaign](models/Amazon%20Adverstising/Sponsored%20Products/SponsoredProducts_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Products | [SponsoredProducts_PlacementCampaignsReport](models/Amazon%20Adverstising/Sponsored%20Products/SponsoredProducts_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Products | [SponsoredProducts_ProductAdsReport](models/Amazon%20Adverstising/Sponsored%20Products/SponsoredProducts_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SponsoredProducts_SearchTermKeywordReport](models/Amazon%20Adverstising/Sponsored%20Products/SponsoredProducts_SearchTermKeywordReport.sql)| A list of product search keywords report |

## Resources:
- Have questions, feedback, or need [help](https://meetings.hubspot.com/balaji-kolli/)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a DBT account & connect to Bigquery

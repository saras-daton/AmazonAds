# Amazon Data Modelling
This DBT package models the Amazon Adverstising data coming from [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

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
  - package: daton/amazon_adverstising_bigquery
    version: [">=0.1.0", "<0.3.0"]
```

## Models

This package contains models from the Amazon API which includes Sponsored Brands, Products, Display. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Sponsored Brands | [SponsoredBrands_Portfolio](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_Portfolio.sql)  | A list of portfolios associated with the account |
|Sponsored Brands | [SponsoredBrands_Campaign](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_Campaign.sql)  | A list of campaigns associated with the account |
|Sponsored Brands | [SponsoredBrands_AdGroupsReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_AdGroupsReport.sql)  | A list of ad groups associated with the account |
|Sponsored Brands | [SponsoredBrands_AdGroupsVideoReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_AdGroupsVideoReport.sql)| A list of ad groups related to sponsored brand video associated with the account |
|Sponsored Brands | [SponsoredBrands_PlacementCampaignsReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Brands | [SponsoredBrands_SearchTermKeywordsReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_SearchTermKeywordsReport.sql)| A list of product search keywords report |
|Sponsored Brands | [SponsoredBrands_SearchTermKeywordsVideoReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_SearchTermKeywordsVideoReport.sql)| A list of keywords associated with sponsored brand video |
|Sponsored Display | [SponsoredDisplay_Portfolio](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredDisplay_Portfolio.sql)| A list of portfolios associated with the account |
|Sponsored Display | [SponsoredBrands_Campaign](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredBrands_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Display | [SponsoredDisplay_ProductAdsReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredDisplay_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SponsoredProducts_Portfolio](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredProducts_Portfolio.sql)| A list of portfolios associated with the account |
|Sponsored Products | [SponsoredProducts_Campaign](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredProducts_Campaign.sql)| A list of campaigns associated with the account |
|Sponsored Products | [SponsoredProducts_PlacementCampaignsReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredProducts_PlacementCampaignsReport.sql)| A list of all the placement campaigns associated with the account |
|Sponsored Products | [SponsoredProducts_ProductAdsReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredProducts_ProductAdsReport.sql)| A list of product ads associated with the account |
|Sponsored Products | [SponsoredProducts_SearchTermKeywordReport](https://github.com/daton/amazon_advertising/blob/main/models/SponsoredProducts_SearchTermKeywordReport.sql)| A list of product search keywords report |

# Configuration 

## Required Variables

This package assumes that you have an existing DBT project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.

```
vars:
    raw_projectid: "your_gcp_project"
    raw_dataset: "your_amazon_sp_api_dataset"
```

## Optional Variables

### Currency Conversion 

To enable currency conversion, which produces two columns - conversion_rate, conversion_currency based on the data from the Exchange Rates Connector from Daton.  please add the following in the dbt_project.yml file. By default, it is False.

```
vars:
    currency_conversion_flag: True
```

### Table Exclusions

Setting these table exclusions will remove the modelling enabled for the below tables. By default, these tables are tagged True. 

```
vars:
    sp_flatfilev2settlement: True
```

## Resources:
- Have questions, feedback, or need [help](https://meetings.hubspot.com/balaji-kolli/)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a DBT account & connect to Bigquery

# Ecommerce Connector Package 

A comprehensive package to fetch sales and Ads performance data for all ecommerce sources.i.e. Amazon, Walmart, Instacart and other retailers.

## 🚀 Overview

This project gives a ready-made framework to fetch sales data and Ads Performance data from multiple retailers. With a minimum changes, pipelines will be ready to integrate. It features:

- **Amazon Ads Data Extraction**: Extraction of Amazon Ads report type API.
- **Amazon Marketing Cloud Extraction**: Designed to run AMC queries and data extraction.
- **Walmart Data Feeds Extraction**: Walmart Omni Sales data ingestion process.
- **Instacart Integration**: Instacart Data Ingestion process.


## 📁 Project Structure

```

├── connectors
│   ├── dist
│   │   ├── ecomm_source_connector-0.1.0-py3-none-any.whl
│   │   └── ecomm_source_connector-0.1.0.tar.gz
│   ├── ecomm_source_connector.egg-info
│   │   ├── dependency_links.txt
│   │   ├── PKG-INFO
│   │   ├── SOURCES.txt
│   │   └── top_level.txt
│   ├── MANIFEST.in
│   ├── requirements.txt
│   ├── setup.py
│   └── src
│       ├── ecomm
│       │   ├── config
│       │   │   ├── instacart_config
│       │   │   │   └── config.yml
│       │   │   └── walmart_config
│       │   │       ├── config.yml
│       │   │       ├── item_attributes.yml
│       │   │       ├── omni_sales_new.yml
│       │   │       ├── omni_sales.yml
│       │   │       ├── product_dimensions.yml
│       │   │       └── store_dimensions.yml
│       │   ├── instacart
│       │   │   ├── __init__.py
│       │   │   ├── chc_ecommerce_instacart_ecs.py
│       │   │   └── instacart_main.py
│       │   └── walmart
│       │       ├── __init__.py
│       │       ├── chc_ecommerce_walmart_ecs.py
│       │       └── walmart_main.py
│       ├── ecomm_source_connector.egg-info
│       │   ├── dependency_links.txt
│       │   ├── PKG-INFO
│       │   ├── SOURCES.txt
│       │   └── top_level.txt
│       ├── media
│       │   ├── amazon_ads
│       │   │   ├── __init__.py
│       │   │   ├── amazon_ads_main.py
│       │   │   └── ecommerce_amzon_ads_ecs.py
│       │   ├── amc
│       │   │   ├── __init__.py
│       │   │   ├── amc_main.py
│       │   │   └── ecommerce_amazon_amc_ecs.py
│       │   └── config
│       │       ├── amazon_ads_config
│       │       │   ├── config.yml
│       │       │   ├── sbAdGroup.yml
│       │       │   ├── sbAds.yml
│       │       │   ├── sbCampaigns.yml
│       │       │   ├── sbPlacement.yml
│       │       │   ├── sbPurchasedProduct.yml
│       │       │   ├── sbTargeting.yml
│       │       │   ├── sdAdvertisedProduct.yml
│       │       │   ├── sdCampaigns.yml
│       │       │   ├── sdTargeting.yml
│       │       │   ├── spAdvertisedProduct.yml
│       │       │   ├── spCampaigns.yml
│       │       │   ├── spPurchasedProduct.yml
│       │       │   └── spTargeting.yml
│       │       └── amc_config
│       │           ├── campaign_LTV.yml
│       │           ├── config.yml
│       │           ├── overlap_ads.yml
│       │           ├── path_to_purchase.yml
│       │           └── sns.yml
│       └── utils
│           ├── __init__.py
│           ├── common_utils.py
│           ├── config_manager.py
│           └── report_processor.py
└── README.md

```
...
## 🛠️ Features
The project is divided into two separate streams. ECOMM folder has the codes for Sources related to Sales. Such as Walmart, Instacart. Whereas, Media contains Ads Performance related codes.,

### 1. Integration of Retailers with Minimum Effort
- With minimum changes in the credentials, code will be ready to pull the data

### 2. Ease of use
- All the sources are modularized with zero dependencies on each other. Each module could be used separately.

### 3. Cloud Integration
- Code can be easily leveraged to integrate with multiple platforms and containers.


##  Prerequisites

- Python 3.6+
- installation of requirements.txt
- AWS IAM role needs to be configured(Bucket Access, ECS Access)

##  Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd connectors/dist
   ```

2. **Install whl file**
   ```bash
   pip install ecomm_source_connector-0.1.0-py3-none-any.whl
   ```
3. **Parameter Installation**
  Currently the codes are designed to run along with the parameters. No addtional steps are needed. In actual scenario, all the parameters needs to be setup in the environment for data security.

## 🚀 Usage
This accelrator can be used for both Media and Sales 

### 1. Amazon Ads Data Extraction
Run the amazon_ads_main.py to extract amazon ads related reports:

```bash
python3 -m src.amazon_ads.amazon_ads_main \
  --report_type 'your_report_type' \
  --start_date 'YYYY-MM-DD' \
  --end_date 'YYYY-MM-DD' \
  --country_code 'US' \
  --client_id 'your_client_id' \
  --client_secret 'your_client_secret' \
  --refresh_token 'your_refresh_token' \
  --config_path config/amazon_ads/config.yml \
  --bucket_nm your_bucket_name
```

### 2. AMC Data Extraction
Run the amazon_ads_main.py to extract amazon ads related reports:

```bash
python3 -m src.amc.amc_main \
  --report_type 'your_report_type' \
  --start_date 'YYYY-MM-DD' \
  --end_date 'YYYY-MM-DD' \
  --country_code 'US' \
  --client_id 'your_client_id' \
  --client_secret 'your_client_secret' \
  --refresh_token 'your_refresh_token' \
  --config_path config/amc_config/config.yml \
  --bucket_nm your_bucket_name
```

##  Parameters List
```
| Argument          | Description                            | Example                        |
| ----------------- | -------------------------------------- | ------------------------------ |
| `--report_type`   | Type of report to generate             | `summary`, `daily`             |
| `--start_date`    | Start date for the report (YYYY-MM-DD) | `2025-08-01`                   |
| `--end_date`      | End date for the report (YYYY-MM-DD)   | `2025-08-05`                   |
| `--country_code`  | Country code in uppercase              | `US`                           |
| `--client_id`     | OAuth2 client ID                       | `your_client_id`               |
| `--client_secret` | OAuth2 client secret                   | `your_client_secret`           |
| `--refresh_token` | OAuth2 refresh token                   | `your_refresh_token`           |
| `--config_path`   | Path to the config YAML file           | `config/amc_config/config.yml` |
| `--bucket_nm`     | Name of the target cloud bucket        | `my-bucket`                    |

```

##  Output Files
Output will be generated in the mentioned bucket in CSV format.


## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request



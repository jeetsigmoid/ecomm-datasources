# Ecommerce Connector Package 

A comprehensive package to fetch sales and Ads performance data for all ecommerce sources.i.e. Amazon, Walmart, Instacart and other retailers.

## 🚀 Overview

This project gives a ready-made framework to fetch sales data and Ads Performance data from multiple retailers. With a minimum changes, pipelines will be ready to integrate. It features:

- **Amazon Ads Data Extraction**: Automated extraction of sales order data from SAP VA05 transaction
- **Amazon Marketing Central Extraction**: Intelligent data analysis using GPT-4 with custom tools
- **Walmart Data Feeds Extraction**: Generation of PDF reports with visualizations
- **Instacart Integration**: SQLite database for data storage and querying


## 📁 Project Structure

```
└── ecomm_source_connector
    ├── dist
    │   ├── ecomm_source_connector-0.1.0-py3-none-any.whl
    │   └── ecomm_source_connector-0.1.0.tar.gz
    ├── ecomm_source_connector.egg-info
    │   ├── dependency_links.txt
    │   ├── PKG-INFO
    │   ├── SOURCES.txt
    │   └── top_level.txt
    ├── MANIFEST.in
    ├── README.md
    ├── requirements.txt
    ├── setup.py
    └── src
        ├── amazon_ads
        │   ├── __init__.py
        │   ├── amazon_ads_main.py
        │   └── ecommerce_amzon_ads_ecs.py
        ├── amc
        │   ├── __init__.py
        │   ├── amc_main.py
        │   └── ecommerce_amazon_amc_ecs.py
        ├── avc
        ├── config
        │   └── amc_config
        │       ├── campaign_LTV.yml
        │       ├── config.yml
        │       ├── overlap_ads.yml
        │       ├── path_to_purchase.yml
        │       └── sns.yml
        ├── ecomm_source_connector.egg-info
        │   ├── dependency_links.txt
        │   ├── PKG-INFO
        │   ├── SOURCES.txt
        │   └── top_level.txt
        ├── instacart
        ├── utils
        │   ├── __init__.py
        │   ├── common_utils.py
        │   ├── config_manager.py
        │   └── report_processor.py
        └── walmart
...
## 🛠️ Features

### 1. Integration of Retailers with Minimum Effort
- With minimum changes in the credentials, code will be ready to pull the data

### 2. Ease of use
- All the sources are modularized with zero dependencies on each other. Each module could be used separately.

### 3. Cloud Integration
- Code can be easily leveraged to integrate with multiple platforms and containers.


##  Prerequisites

- Python 3.6+
- installation of requirements.txt

##  Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ecomm_source_connector
   ```
2. **Setup Git Parameters**
   ```bash
   ```

3. **Install dependencies**
   ```bash
   pip install ecomm_source_connector-0.1.0-py3-none-any.whl
   ```

3. **Configure environment variables**
   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   ```

##  Configuration


## 🚀 Usage

### 1. Amazon Ads Data Extraction
Run the amazon_ads_main.py to extract amazon ads related reports:

```bash
python src/amazon_ads/amazon_ads_main.py
```

### 2. AMC Data Extraction
Run the amazon_ads_main.py to extract amazon ads related reports:

```bash
python src/amc/amc_main.py
```

##  Customization


##  Output Files

The system generates:
- `output/open_inprocess_percentage.csv` - Open order percentages
- `output/open_inprocess_aging.csv` - Aging analysis
- `output/overdue_open_orders.csv` - Overdue orders
- `summary_report.pdf` - Comprehensive business report
- `material_sold_units_report_Total Sold Units_barplot.png` - Sales visualization



## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request



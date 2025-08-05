# SAP Agentic Process Automation POC

A comprehensive proof-of-concept project that demonstrates intelligent automation of SAP business processes using Large Language Models (LLMs) and RPA (Robotic Process Automation) techniques.

## 🚀 Overview

This project combines SAP automation with AI-powered data analysis to create intelligent business process automation. It features:

- **SAP Data Extraction**: Automated extraction of sales order data from SAP VA05 transaction
- **LLM-Powered Analysis**: Intelligent data analysis using GPT-4 with custom tools
- **Automated Reporting**: Generation of PDF reports with visualizations
- **SQL Database Integration**: SQLite database for data storage and querying
- **Azure Integration**: Blob storage and email capabilities

## 📁 Project Structure

```
agentic-process-automation-sap/
├── amazon_ads/
│   ├── sap/           # SAP automation modules
│   │   ├── sap.py     # SAP connection and login
│   │   └── va05.py    # VA05 transaction automation
│   ├── rpa/           # RPA orchestration
│   │   └── rpa.py     # Main RPA workflow
│   └── utils/         # Utility modules
│       ├── blob_uploader.py  # Azure blob storage
│       ├── Mailer.py         # Email functionality
│       └── utils.py          # General utilities
├── tools/             # LLM tools for data processing
│   ├── generic_plot_tool.py      # Chart generation
│   ├── pdf_report_tool.py        # PDF report creation
│   └── pdf_report_tool_new.py    # Enhanced PDF reporting
├── prompt/            # SQL analysis prompts
│   └── prompt.py      # Predefined analysis queries
├── output/            # Generated reports and data
├── llm_tool_integration.py       # LLM with tool integration
├── sql_lite_agent_v3.py          # SQL database agent
├── requirements.txt               # Python dependencies
└── README.md                     # This file
```

## 🛠️ Features

### 1. SAP Automation
- **Automated Login**: Secure SAP GUI automation with credential management
- **VA05 Transaction**: Automated extraction of sales order data
- **Data Export**: CSV export with configurable date ranges

### 2. Intelligent Data Analysis
- **SQL Agent**: LLM-powered SQL query generation and execution
- **Custom Analysis**: Predefined prompts for business metrics:
  - Open/In-Process order percentages
  - Aging bucket analysis
  - Overdue order identification

### 3. Visual Reporting
- **Bar Charts**: Automated generation of material sales visualizations
- **PDF Reports**: Comprehensive business reports with charts and metrics
- **Data Export**: CSV outputs for further analysis

### 4. Cloud Integration
- **Azure Blob Storage**: Automated file upload to cloud storage
- **Email Notifications**: Automated report distribution via email

##  Prerequisites

- Python 3.8+
- SAP GUI installed and configured
- SAP system access credentials
- Azure storage account (optional)
- OpenAI API key

##  Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd agentic-process-automation-sap
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   ```

##  Configuration


## 🚀 Usage

### 1. SAP Data Extraction
Run the main RPA workflow to extract SAP data:

```bash
python src/rpa/rpa.py
```

### 2. LLM-Powered Analysis
Execute intelligent data analysis:

```bash
python sql_lite_agent_v3.py
```

### 3. Generate Reports with LLM Tools
Create visualizations and PDF reports:

```bash
python llm_tool_integration.py
```

##  Analysis Capabilities

The system provides automated analysis for:

1. **Open Order Percentage**: Calculate percentage of open and in-process orders
2. **Aging Analysis**: Categorize orders by aging buckets (0-30, 31-60, 61-90, 90+ days)
3. **Overdue Orders**: Identify orders with delivery dates before creation dates
4. **Material Sales**: Top materials analysis with visualizations

##  Customization

### Adding New Analysis Prompts
Add new prompts to `prompt/prompt.py`:

```python
new_prompt = """
You are a SQL data analyst.
[Your analysis requirements here]
"""
```

### Creating New Tools
Extend the LLM capabilities by adding new tools in the `tools/` directory.

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



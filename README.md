# Snowflake-Mailing

Automated hotel recommendation emailing system powered by Snowflake and OpenAI.

## Overview

This application connects to a Snowflake database to identify target customers, analyze their preferences, generate personalized hotel recommendations, and create customized emails. The system uses SQL queries via Snowflake and natural language generation via OpenAI.

## Features

- 🔍 Identifies target customers based on configurable criteria
- 🏨 Generates personalized hotel recommendations for each customer
- ✉️ Creates custom emails with tailored content 
- 📊 Tracks performance metrics for each processing stage
- 💾 Stores generated emails in timestamped directories

## Setup

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Create a `.env` file with the following variables:
   ```
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   SNOWFLAKE_HOST=your_host
   SNOWFLAKE_ROLE=your_role
   OPENAI_API_KEY=your_openai_key
   ```

## Environment Setup (Example)

Here's a detailed example of how to set up your environment for this project:

1. Create and activate a virtual environment:
   ```bash
   # Create virtual environment
   python -m venv venv
   
   # Activate on macOS/Linux
   source venv/bin/activate
   
   # Activate on Windows
   venv\Scripts\activate
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up Snowflake configuration:
   - Option 1: Create a `.env` file in the project root with your Snowflake and OpenAI credentials:
     ```
     # Snowflake Connection Parameters
     SNOWFLAKE_USER=your_snowflake_username
     SNOWFLAKE_PASSWORD=your_secure_password
     SNOWFLAKE_ACCOUNT=your_account_id.region
     SNOWFLAKE_WAREHOUSE=AI_WH
     SNOWFLAKE_DATABASE=DAYUSE_ANALYTICS
     SNOWFLAKE_SCHEMA=STREAMLIT_APPS
     SNOWFLAKE_HOST=your_account_id.region.snowflakecomputing.com
     SNOWFLAKE_ROLE=DEV_ROLE
     
     # OpenAI API Key
     OPENAI_API_KEY=sk-your_openai_api_key
     ```
   
   - Option 2: Use Snowflake CLI configuration:
     The project comes with a `.snowflake/config.toml` that can be used with Snowflake CLI.
     Update this file with your credentials:
     ```toml
     default_connection_name = "default"
     
     [connections.default]
     account = "your_account_id.region"
     user = "your_username"
     password = "your_password"
     warehouse = "AI_WH"
     database = "DAYUSE_ANALYTICS"
     schema = "STREAMLIT_APPS"
     host = "your_account_id.region.snowflakecomputing.com"
     role = "DEV_ROLE"
     ```

4. Verify generated emails directory:
   ```bash
   # The application will save emails to this directory
   mkdir -p src/gen_mails
   ```

5. Verify your environment is correctly configured:
   ```bash
   # Test Snowflake connection and required packages
   python -c "import snowflake.connector; import openai; import pandas; from dotenv import load_dotenv; print('Environment successfully configured!')"
   ```

6. Run a test query (optional):
   ```bash
   python -c "import os; from dotenv import load_dotenv; import snowflake.connector; load_dotenv(); conn = snowflake.connector.connect(user=os.getenv('SNOWFLAKE_USER'), password=os.getenv('SNOWFLAKE_PASSWORD'), account=os.getenv('SNOWFLAKE_ACCOUNT'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'), database=os.getenv('SNOWFLAKE_DATABASE'), schema=os.getenv('SNOWFLAKE_SCHEMA')); cur = conn.cursor(); cur.execute('SELECT current_version()').fetchone(); print('Snowflake connection successful!')"
   ```

7. Troubleshooting:
   - Snowflake connection issues: Verify your account URL format and credentials
   - OpenAI API issues: Check API key validity and quota
   - File permissions: Ensure the application has write permissions for the `src/gen_mails` directory
   - If using Streamlit features: Run with `streamlit run src/Mailing_app.py` instead

## Usage

Run the mailing application:

```bash
python src/Mailing_app.py
```

The application will:
1. Identify target customers 
2. Generate hotel recommendations for each customer
3. Create personalized emails
4. Save emails to `gen_mails/gen_mail_YYYY-MM-DD_HH-MM-SS/`

## Output

Emails are saved as JSON files in timestamped directories:
- Each file is named with the customer ID and name
- Files contain the email content and performance metrics
- Performance metrics track time for each processing stage 
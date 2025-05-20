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
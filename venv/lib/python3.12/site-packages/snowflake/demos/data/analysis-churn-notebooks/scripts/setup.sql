USE ROLE securityadmin;

-- create churn_data_scientist
CREATE OR REPLACE ROLE churn_data_scientist;

USE ROLE accountadmin;

/*---------------------------*/
-- Create our Database
/*---------------------------*/
CREATE OR REPLACE DATABASE churn_prod;

/*---------------------------*/
-- Create our Schema
/*---------------------------*/
CREATE OR REPLACE SCHEMA churn_prod.analytics;

/*---------------------------*/
-- Create our Warehouse
/*---------------------------*/

-- data science warehouse
CREATE OR REPLACE WAREHOUSE churn_ds_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data science warehouse for churn prediction';

-- Use our Warehouse
USE WAREHOUSE churn_ds_wh;

-- grant churn_ds_wh priviledges to churn_data_scientist role
GRANT USAGE ON WAREHOUSE churn_ds_wh TO ROLE churn_data_scientist;
GRANT OPERATE ON WAREHOUSE churn_ds_wh TO ROLE churn_data_scientist;
GRANT MONITOR ON WAREHOUSE churn_ds_wh TO ROLE churn_data_scientist;
GRANT MODIFY ON WAREHOUSE churn_ds_wh TO ROLE churn_data_scientist;

-- grant churn_ds_wh database privileges
GRANT ALL ON DATABASE churn_prod TO ROLE churn_data_scientist;

GRANT ALL ON SCHEMA churn_prod.analytics TO ROLE churn_data_scientist;
GRANT CREATE STAGE ON SCHEMA churn_prod.analytics TO ROLE churn_data_scientist;

GRANT ALL ON ALL STAGES IN SCHEMA churn_prod.analytics TO ROLE churn_data_scientist;
GRANT ALL ON ALL STAGES IN SCHEMA churn_prod.public TO ROLE churn_data_scientist;

-- set my_user_var variable to equal the logged-in user
SET my_user_var = (SELECT  '"' || CURRENT_USER() || '"' );

-- grant the logged in user the churn_data_scientist role
GRANT ROLE churn_data_scientist TO USER identifier($my_user_var);

USE ROLE churn_data_scientist;

/*---------------------------*/
-- sql completion note
/*---------------------------*/
SELECT 'data analysis and churn prediction sql is now complete' AS note;

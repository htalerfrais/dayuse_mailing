USE ROLE securityadmin;
DROP ROLE IF EXISTS churn_data_scientist;
USE ROLE accountadmin;
DROP DATABASE IF EXISTS churn_prod;
DROP WAREHOUSE IF EXISTS churn_ds_wh;

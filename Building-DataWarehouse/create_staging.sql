-- create database which contain multiple schema
-- 1. staging schema
-- 2. ODS Schema
-- 3. DWH Schema
---------------------------------------------------------------------------------------------------------
-- drop database if exists
DROP DATABASE RETAILDEMO
-- create databaseه
CREATE DATABASE RETAILDEMO;
-- use database
use database RETAILDEMO;
-- create staging schema
--drop schema STAGING
CREATE SCHEMA "RETAILDEMO"."RETAIL_STAGING";
-- create ODS schema
CREATE SCHEMA "RETAILDEMO"."RETAIL_ODS";
-- create DWH schema
CREATE SCHEMA "RETAILDEMO"."RETAIL_DWH";
---------------------------------------------------------------------------------------------------------
-- Create csv file format
create
or replace file format retail_csvFormat type = 'CSV' compression = 'auto' field_delimiter = ',' record_delimiter = '\n' skip_header = 1 error_on_column_count_mismatch = true null_if = ('NULL', 'null') empty_field_as_null = true;
-- Create json file format
create
or replace file format retail_json_format TYPE = 'JSON' COMPRESSION = 'AUTO' ---------------------------------------------------------------------------------------------------------
-- create staging area to map data from datalake to specific staging
-- 1. stores stage
CREATE STAGE stores_stage URL = 's3://retail-sources/stores data-set/stores data-set.csv' CREDENTIALS = (
    AWS_KEY_ID = 'AKIAW5AFKK2PBX6MCT4M',
    AWS_SECRET_KEY = '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'
) FILE_FORMAT = retail_csvFormat;
list @stores_stage -- 2. sales stage
CREATE STAGE sales_stage URL = 's3://retail-sources/sales data-set/sales data-set.csv' CREDENTIALS = (
    AWS_KEY_ID = 'AKIAW5AFKK2PBX6MCT4M',
    AWS_SECRET_KEY = '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'
) FILE_FORMAT = retail_csvFormat;
list @sales_stage -- 3. features stage
drop stage features_stage CREATE STAGE features_stage URL = 's3://retail-sources/Features_dataset/Features data set.csv' CREDENTIALS = (
    AWS_KEY_ID = 'AKIAW5AFKK2PBX6MCT4M',
    AWS_SECRET_KEY = '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'
) FILE_FORMAT = retail_csvFormat;
list @features_stage ---------------------------------------------------------------------------------------------------------
-- create table instaging area to ingest data into tables from csv file
-- retail_sales
USE DATABASE "RETAILDEMO";
USE SCHEMA "RETAILDEMO"."RETAIL_STAGING";
drop table RETAIL_SALES CREATE
OR REPLACE TABLE RETAIL_SALES (
    Store int,
    Dept int,
    Date string,
    Weekly_Sales float,
    IsHoliday boolean
);
-- cpy data from staging file into sales table
COPY INTO RETAIL_SALES
FROM
    '@sales_stage' FILE_FORMAT = retail_csvFormat PURGE = TRUE;
select
    *
from
    retail_sales
limit
    4;
select
    count(*)
from
    retail_sales -- copy date from staging file into stores table
    USE SCHEMA "RETAILDEMO"."RETAIL_STAGING";
CREATE
    OR REPLACE TABLE RETAIL_STORES (
        Store int,
        Type character,
        size int
    );
COPY INTO RETAIL_STORES
FROM
    '@stores_stage' FILE_FORMAT = retail_csvFormat PURGE = TRUE;
select
    *
from
    RETAIL_STORES
limit
    4;
select
    count(*)
from
    RETAIL_STORES -- copy data from staging files into features table
    USE SCHEMA "RETAILDEMO"."RETAIL_STAGING";
CREATE
    OR REPLACE TABLE RETAIL_FEATURES (
        Store int,
        Date text,
        Temperature FLOAT,
        Fuel_Price FLOAT,
        MarkDown1 text,
        MarkDown2 text,
        MarkDown3 text,
        MarkDown4 text,
        MarkDown5 text,
        CPI text,
        Unemployment text,
        IsHoliday boolean
    );
COPY INTO RETAIL_FEATURES
FROM
    '@features_stage' FILE_FORMAT = retail_csvFormat PURGE = TRUE;
select
    *
from
    RETAIL_FEATURES;
select
    count(*)
from
    RETAIL_FEATURES

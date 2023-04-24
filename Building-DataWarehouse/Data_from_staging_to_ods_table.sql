use database retaildemo
use schema retail_ods

-- ingest data into ODS TABLES

--  insert data into store table
insert into retail_ods.store(TYPE , SIZE) 
select RTS.TYPE , RTS.SIZE from retail_staging.retail_stores  RTS

SELECT * FROM retail_ods.store

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- INSERT DATA into Date Table
insert into retail_ods.date(date)

SELECT concat(SUBSTRING(RTF.DATE, 1, CHARINDEX('/', RTF.DATE) - 1) , '-' , SUBSTRING(RTF.DATE, CHARINDEX('/', RTF.DATE) + 1, CHARINDEX('/', RTF.DATE, CHARINDEX('/', RTF.DATE) + 1) - CHARINDEX('/', RTF.DATE) - 1) , '-' ,SUBSTRING(RTF.DATE, CHARINDEX('/', RTF.DATE, CHARINDEX('/', RTF.DATE) + 1) + 1, LEN(RTF.DATE) - CHARINDEX('/', RTF.DATE, CHARINDEX('/', RTF.DATE) + 1))) as date
from retail_staging.retail_features RTF 


select * from retail_ods.date

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- INSERT DATA into markdown Table
insert into retail_ods.markdown(store_id,markdown1,markdown2,markdown3,markdown4,markdown5)
select  RTF.STORE,
    CASE
       WHEN RTF.MARKDOWN1 = 'NA' THEN CAST(0.0 AS FLOAT) 
       ELSE  RTF.MARKDOWN1
    END as MARKDOWN1
    , 
    CASE
       WHEN RTF.MARKDOWN2 = 'NA' THEN CAST(0.0 AS FLOAT) 
       ELSE RTF.MARKDOWN2
    END as MARKDOWN2
    , 
    CASE
       WHEN RTF.MARKDOWN3 = 'NA' THEN CAST(0.0 AS FLOAT) 
       ELSE RTF.MARKDOWN3
    END as MARKDOWN3
    , 
    CASE
       WHEN RTF.MARKDOWN4 = 'NA' THEN CAST(0.0 AS FLOAT) 
       ELSE RTF.MARKDOWN4
    END as MARKDOWN4
    , 
    CASE
       WHEN RTF.MARKDOWN5 = 'NA' THEN CAST(0.0 AS FLOAT) 
       ELSE RTF.MARKDOWN5
    END as MARKDOWN5 
   

from retail_staging.retail_features RTF 


select * from retail_ods.markdown 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- insert into Holiday table 
INSERT INTO RETAIL_ODS.ISHOLIDAY(STORE_ID , ISHOLIDAY)
select RTF.STORE , RTF.ISHOLIDAY from retail_staging.retail_features RTF

select * from RETAIL_ODS.ISHOLIDAY 


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- insert into Department table 
insert into retail_ods.department(DEPARTMENT_ID , STORE_ID , DATE_ID , WEEKLY_SALES )

with base as 
(
select * from retail_ods.date RTD
join 
(
select RTS.DEPT ,RTS.STORE, RTS.WEEKLY_SALES ,
concat(SUBSTRING(RTS.DATE, 1, CHARINDEX('/', RTS.DATE) - 1) , '-' , SUBSTRING(RTS.DATE, CHARINDEX('/', RTS.DATE) + 1, CHARINDEX('/', RTS.DATE, CHARINDEX('/', RTS.DATE) + 1) - CHARINDEX('/', RTS.DATE) - 1) , '-' ,SUBSTRING(RTS.DATE, CHARINDEX('/', RTS.DATE, CHARINDEX('/', RTS.DATE) + 1) + 1, LEN(RTS.DATE) - CHARINDEX('/', RTS.DATE, CHARINDEX('/', RTS.DATE) + 1))) as date
from retail_staging.retail_sales RTS 
) sales

on sales.date = RTD.DATE
)

select base.dept , base.store , base.date_id , weekly_sales from base 


select * from retail_ods.department limit 200



-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- insert into features table 

insert into retail_ods.features(STORE_ID , DATE_ID , MARKDOWN_ID , ISHOLIDAY_ID , temperature , FUELPRICE , cpi , unemployment)

with base as 
(
WITH feature as 
(
    SELECT RTF.STORE , RTF.TEMPERATURE , RTF.FUEL_PRICE , RTF.CPI , RTF.UNEMPLOYMENT,RTF.ISHOLIDAY,
    concat(SUBSTRING(RTF.DATE, 1, CHARINDEX('/', RTF.DATE) - 1) , '-' , SUBSTRING(RTF.DATE, CHARINDEX('/', RTF.DATE) + 1, CHARINDEX('/', RTF.DATE, CHARINDEX('/', RTF.DATE) + 1) - CHARINDEX('/',RTF.DATE) - 1) ,
    '-' ,SUBSTRING(RTF.DATE, CHARINDEX('/', RTF.DATE, CHARINDEX('/', RTF.DATE) + 1) + 1, LEN(RTF.DATE) - CHARINDEX('/', RTF.DATE, CHARINDEX('/', RTF.DATE) + 1))) as date
    FROM retail_staging.retail_features RTF 
)

select * from feature 

INNER JOIN retail_ods.isholiday RTH
    ON RTH.ISHOLIDAY = feature.ISHOLIDAY

INNER JOIN retail_ods.date RTD
    on RTD.DATE =  feature.DATE

INNER JOIN retail_ods.markdown RTMK
    on RTMK.STORE_ID = feature.STORE
) 

select base.store , base.date_id, base.markdown_id , base.ISHOLIDAY_ID , base.temperature , base.fuel_price , base.cpi , base.unemployment from base


SELECT * FROM retail_ods.features LIMIT 4


-- ingest data from ODS TO DWH 

-- INGEST DATE from ODS to DATE DIM

use database retaildemo 
use schema retail_dwh

insert into retail_dwh.date_dim(date)
select date from retail_ods.date 


select * from retail_dwh.date_dim

-----------------------------------------------------------------------------------------------------------------------------------------------------------
-- INGEST stores from ODS to store DIM
insert into retail_dwh.store_dim(type , size)
select Type , size from retail_ods.store 

-----------------------------------------------------------------------------------------------------------------------------------------------------------
-- INGEST department from ODS to department DIM
insert into retail_dwh.department_dim 
select DEPARTMENT_ID from retail_ods.department 


-----------------------------------------------------------------------------------------------------------------------------------------------------------
-- INGEST features from ODS to features DIM
INSERT INTO RETAIL_DWH.FEATURES_DIM(fuelprice , cpi , temperature , unemployment , isholiday , markdown1 , markdown2 , markdown3 , markdown4 , markdown5)
select RTF.FUELPRICE , RTF.CPI , RTF.TEMPERATURE , RTF.UNEMPLOYMENT ,RTOH.ISHOLIDAY , RTMK.MARKDOWN1 , RTMK.MARKDOWN2 , RTMK.MARKDOWN3 , RTMK.MARKDOWN4 , RTMK.MARKDOWN5
from 
    retail_ods.features RTF 

    JOIN retail_ods.isholiday RTOH
        on RTOH.ISHOLIDAY_ID = RTF.ISHOLIDAY_ID
        
    JOIN retail_ods.markdown RTMK
        ON RTMK.MARKDOWN_ID = RTF.MARKDOWN_ID

select * from RETAIL_DWH.FEATURES_DIM limit 5


-----------------------------------------------------------------------------------------------------------------------------------------------------------
-- INGEST fact from ODS to fact DIM


INSERT INTO RETAIL_DWH.SALES_FACT(WEEKLY_SALES , ISHOLIDAY , DATE_ID , STORE_ID , DEPARTMENT_ID )

select DISTINCT(RTD.WEEKLY_SALES)  , RTH.ISHOLIDAY , ROD.DATE_ID , RST.STORE_ID , RTD.DEPARTMENT_ID
from retail_ods.department RTD 

LEFT JOIN retail_ods.isholiday RTH
    on 
        RTH.STORE_ID = RTD.STORE_ID
LEFT join retail_ods.date ROD
    ON 
        ROD.DATE_ID =  RTD.DATE_ID
LEFT JOIN RETAIL_ODS.STORE RST
    ON 
        RST.STORE_ID = RTD.STORE_ID


select * from RETAIL_DWH.SALES_FACT  limit 5

------------------------------------ REPORTING ------------------------------------------------------
-- DROP TABLE weakly_sales_report
create table weakly_sales_report as 

select RDS.weekly_sales ,RDS.ISHOLIDAY ,RTS.TYPE, RTS.SIZE,  RTD.DATE  
from retail_dwh.sales_fact RDS 
    join retail_dwh.date_dim RTD
        on RTD.DATE_ID = RDS.DATE_ID
    JOIN retail_dwh.store_dim RTS
        on RTS.STORE_ID = RDS.STORE_ID
        
select * from weakly_sales_report limit 4







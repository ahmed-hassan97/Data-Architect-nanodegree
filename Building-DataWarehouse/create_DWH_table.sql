-- create dimention table in DWH
use database retaildemo
use schema retail_dwh


-- Create date_dim
create or replace table Date_dim(
    date_id int PRIMARY KEY  IDENTITY,
    date string
)

-- create store dim

create or replace table store_dim(
    store_id int PRIMARY KEY  IDENTITY,
    Type char ,
    size int
)

-- create department dim 
create or replace table department_dim(
    
    department_id int PRIMARY KEY  IDENTITY
   
)

-- create feature dimention 
create or replace table features_dim(
    feature_id int PRIMARY KEY  IDENTITY,
    temperature float,
    fuelPrice Float,
    cpi float,
    unemployment float,
    IsHoliday boolean,
    MarkDown1 float,
    MarkDown2 float,
    MarkDown3 float,
    MarkDown4 float,
    MarkDown5 float
)


-- create fact table 
create or replace table sales_fact(
    Weekly_Sales float ,
    IsHoliday boolean,
    date_id int ,
    store_id int,
    department_id int,
    feature_id int,
    
   CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id),
   CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES Date (date_id),
   CONSTRAINT fk_department FOREIGN KEY (department_id) REFERENCES department (department_id) ,
   CONSTRAINT fk_feature FOREIGN KEY (feature_id) REFERENCES features (feature_id)  

   
)

select * from sales_fact

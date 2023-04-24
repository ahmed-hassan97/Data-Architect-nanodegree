use database retaildemo
use schema retail_ods
-- create ods table with all relation 

-- create store 
create or replace table store(
    store_id int PRIMARY KEY  IDENTITY,
    Type char ,
    size int
)

-- create holiday 
create or replace table isHoliday(
    isHoliday_id int PRIMARY KEY  IDENTITY,
    isHoliday boolean ,
    store_id  int,
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id)
)

-- create Date 
create or replace table Date(
    date_id int PRIMARY KEY  IDENTITY,
    date string
)

-- create markdown
create or replace table markdown(
    markdown_id int PRIMARY KEY  IDENTITY,
    store_id int,
    markdown1 float,
    markdown2 float,
    markdown3 float,
    markdown4 float,
    markdown5 float ,
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id)
)

-- create department table
create or replace table department(
    id int PRIMARY KEY  IDENTITY,
    department_id int,
    store_id int,
    date_id int,
    weekly_sales float,
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES Date (date_id)   
)

-- create features table
create or replace table features(
    feature_id int PRIMARY KEY  IDENTITY,
    store_id int,
    date_id int,
    markdown_id int,
    isHoliday_id int,
    temperature float,
    fuelPrice Float,
    cpi float,
    unemployment float,
  
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES Date (date_id)  ,
    CONSTRAINT fk_markdown FOREIGN KEY (markdown_id) REFERENCES markdown (markdown_id) ,
    CONSTRAINT fk_isHoliday FOREIGN KEY (isHoliday_id) REFERENCES isHoliday (isHoliday_id) 
)



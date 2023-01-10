--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 1, Creating the role Hierarchy
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

use role accountadmin; 

create role admin;

create role developer;

grant role admin to role accountadmin;

grant role developer to role admin;

show grants to role admin;

show grants to role developer;

create role pii;

grant role pii to role accountadmin;


--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 2. Creating the data warehouse of size Medium 
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

use role accountadmin; 

CREATE WAREHOUSE assignment_wh WITH WAREHOUSE_SIZE = Medium;

grant usage on warehouse assignment_wh to role admin;


--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 3. switch to admin role
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

use role accountadmin; 

grant create database on account to role admin;

use role admin;


--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 4. Creating the Database of name assignment_db
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

use role admin;

create or replace database assignment_db;

use assignment_db;
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 5. Creating the schema named my_schema
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

CREATE or replace SCHEMA my_schema;



--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 6. Creating the stage and table
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

create or replace stage my_stage
  file_format = (type = csv field_delimiter=',' skip_header=1);

CREATE or replace TABLE assignment_db.my_schema.my_table
(
    index int,
    user_id string,
    first_name string,
    last_name string,
    sex string,
    email string,
    phone string, 
    date_of_birth date,
    job_title string,
    elt_ts timestamp,
    file_name string,
    elt_by string
);





create or replace file format my_csv_format
type = csv field_optionally_enclosed_by='"' field_delimiter = ',' record_delimiter = '\n' error_on_column_count_mismatch = false
null_if = ('null', 'null') empty_field_as_null = true
skip_header = 1;




copy into assignment_db.my_schema.my_table
from(select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, current_timestamp(), metadata$filename, 'local' from  @ASSIGNMENT_DB.MY_SCHEMA.my_stage t )
file_format = my_csv_format
on_error = 'continue';

SELECT * FROM MY_TABLE limit 100;

--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 7. Creating the variant version of the dataset
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

CREATE or replace TABLE my_schema.my_table_variant
(
    index variant,
    user_id variant,
    first_name variant,
    last_name variant,
    sex variant,
    email variant,
    phone variant, 
    date_of_birth variant,
    job_title variant,
    elt_ts variant,
    file_name variant
);


insert into my_table_variant select 
to_variant(index),
to_variant(user_id), 
to_variant(first_name),
to_variant(last_name), 
to_variant(sex), 
to_variant(email), 
to_variant(phone), 
to_variant(date_of_birth), 
to_variant(job_title), 
to_variant(elt_ts), 
to_variant(file_name) 
from my_table;



select * from my_table_variant limit 10;


--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 8. Load the file into an external and internal stage.
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
put file:///Users/vaibhaw/Downloads/people-100000.csv @my_stage;

use role accountadmin;
grant create integration on account to role admin;
use role admin;



create or replace storage integration aws_s3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::511665032808:role/newrole'
  storage_allowed_locations = ('s3://snowflakebucketabc');


desc integration aws_s3;


create or replace stage ASSIGNMENT_DB.MY_SCHEMA.s3_stage
  storage_integration = aws_s3
  url = 's3://snowflakebucketabc/people-100000.csv'
  file_format = my_csv_format;



select t.$3 as first_name,t.$4 last_name,t.$6 email
from @ASSIGNMENT_DB.MY_SCHEMA.s3_stage/ t;


--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 9. Load the file into an external and internal stage.
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------





--------------------------------------------------------------------------------------------------------------------------------------------------
-- 10. Upload any unrelated parquet file to the stage location and infer the schema of the file.
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------




create file format my_parquet_format
  type = parquet;

create or replace stage my_stage_parquet
  file_format = (type = parquet )
  ;


-- Query the INFER_SCHEMA function.
select *
  from table(
    infer_schema(
      location=>'@my_stage_parquet'
      , file_format=>'my_parquet_format'
      )
    );
    
    

--------------------------------------------------------------------------------------------------------------------------------------------------
-- 11. Run a select query on the staged parquet file without loading it to a snowflake table.
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

    
select * from @assignment_db.my_schema.my_stage_parquet limit 10;
 
 

  
  
--------------------------------------------------------------------------------------------------------------------------------------------------
-- 12. Add masking policy to the PII columns such that fields like email, phone
-- number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible.
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

use role accountadmin;

create or replace masking policy assignment_db.my_schema.email_mask as (val string) returns string ->
case
when current_role() in ('DEVELOPER') then '******'
else val
end;

GRANT SELECT ON TABLE assignment_db.my_schema.my_table TO ROLE DEVELOPER;

GRANT USAGE ON WAREHOUSE ASSIGNMENT_WH TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA my_schema TO ROLE Developer;

alter table if exists assignment_db.my_schema.my_table modify column email set masking policy 
assignment_db.my_schema.email_mask;

alter table if exists assignment_db.my_schema.my_table modify column phone set masking policy 
assignment_db.my_schema.email_mask;

use role developer;

select * from assignment_db.my_schema.my_table limit 100;




















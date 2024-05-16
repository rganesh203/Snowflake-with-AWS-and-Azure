use database json_ttips;
use schema channel;

SELECT  * FROM  JSON_TTIPS.CHANNEL.FILE;

create or replace transient table json_tbl(
    json_col variant
);

insert into json_tbl(json_col)
select parse_json(
  '{
    "employee": {
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "married": true,
      "has_kids": false,
      "stock_options": null,
      "phone": [
        "+1 123-456-7890",
        "+1 098-765-4321"
      ],
      "Address" {
        "street": "4132 Grant Street",
        "city": "Rockville",
        "State": "Minnesota"
      }
    }
  }');


create or replace transient table employee_tbl(
    emp_json variant
);

-- let me insert one single record
insert into employee_tbl(emp_json)
select parse_json(
  '{
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "married": true,
      "has_kids": false,
      "stock_options": null
    }');

-- lets run the select sql
select * from employee_tbl;

select 

    emp_json:age::integer as age,
    emp_json:name::string as "Name",
    emp_json:height_in_ft::decimal as height,
    emp_json:married as is_married,
    emp_json:has_kids as has_kids,
    emp_json:stock_options as stock_options
from employee_tbl;

select 
    typeof(emp_json:name) as name,
    typeof(emp_json:age) as age,
    typeof(emp_json:height_in_ft) as height,
    typeof(emp_json:married) as is_married,
    typeof(emp_json:has_kids) as has_kids,
    typeof(emp_json:stock_options) as stock_options
from employee_tbl;

insert into employee_tbl(emp_json)
select parse_json(
  '{
    "employee": {
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "married": true,
      "has_kids": false,
      "stock_options": null,
      "phone": [
        "+1 123-456-7890",
        "+1 098-765-4321"
      ],
      "Address": {
        "street": "3621 McDonald Avenue",
        "city": "Orlando",
        "State": "Florida"
      }
    }
  }');

  select * from employee_tbl;

  select 
    emp_json:employee.name::string as name,
    emp_json:employee.age as age,
    emp_json:employee.height_in_ft as height,
    emp_json:employee.married as is_married,
    emp_json:employee.has_kids as has_kids,
    emp_json:employee.stock_options as stock_options,
    typeof(emp_json:employee.phone) as all_phone_type,
    ARRAY_SIZE(emp_json:employee.phone) as how_many_phone,
    emp_json:employee.phone[0] as work_phone,
    emp_json:employee.phone[1] as office_phone,
    typeof(emp_json:employee:Address) as address_type,
    emp_json:employee:Address:street as street,
    emp_json:employee.Address.city as city,
    emp_json:employee.Address.State as state
from employee_tbl; 
-- apply other function and mathematical operation without casting it.
select 
    emp_json:employee.age as age,
    (emp_json:employee.height_in_ft) * (12*2.54) as height_in_cm,
    typeof(emp_json:employee.Phone) as all_phone_type,
    ARRAY_SIZE(emp_json:employee.Phone) as how_many_phone
from employee_tbl; 
--Date & Timestamp Data Types in Snowflake JSON

insert into employee_tbl(emp_json)
select parse_json(
  '{
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "dob":"2022-12-11",
      "dob_timestemp":"2022-12-11T00:19:06.043-08:00",
      "married": true,
      "has_kids": false,
      "stock_options": null
    }');
    
   
 select 
        emp_json:dob::date,
        emp_json:dob_timestemp::timestamp
        from employee_tbl order by 1 desc;  

--Flatting JSON Data & Loading Into Snowflake Tables

-- create the table

create transient table employee_tbl(
    emp_json variant
);

--1st record
insert into employee_tbl(emp_json)
select parse_json(
  '{
    "employee": {"name": "John-1","age": 30,"height_in_ft": 5.11,"married": true,"has_kids": false,
      "stock_options": null,"email":"john1@ttips.com","phone": ["+1 123-456-7890","+1 098-765-4321"],
      "Address": {"street": "3621 McDonald Avenue","city": "Orlando","State": "Florida"}
               }
    }');
--2nd record
insert into employee_tbl(emp_json)
select parse_json(
  '{
    "employee": {"name": "John-2","age": 33,"height_in_ft": 5.09,"married": false,"has_kids": false,
      "stock_options": 10,"email":"john2@ttips.com","phone": ["+1 222-456-0987"],
      "Address": {"street": "532 Locust View Drive","city": "San Jose","State": "California"}
               }
    }');

--Creating Sequence Objects
-- create sequencer
create or replace sequence emp_seq
  start 1 
  increment 1
  comment = 'employee sequence';
  
create or replace sequence phone_seq
  start 1 
  increment 1
  comment = 'phone sequence';
  
create or replace sequence address_seq
  start 1 
  increment 1
  comment = 'address sequence';

--Creating Master Tables
-- employee master table
create or replace table employee_master(
    emp_pk integer default emp_seq.nextval,
    name string,
    age number(3),
    height_in_cm decimal(6,3),
    is_married boolean,
    has_kids boolean,
    stock_options integer,
    email varchar(100)
);

-- child table holding all the phones
create or replace table emp_phones(
    phone_pk integer default phone_seq.nextval,
    emp_fk number,
    phone_type varchar(20),
    phone_number varchar(30)
);

-- child table holding all the phones
create or replace table emp_address(
    address_pk integer default address_seq.nextval,
    emp_fk number,
    street_address varchar(200),
    city varchar(50),
    state varchar(50)
);
--Insert into employee master table
insert into employee_master (name, age, height_in_cm,is_married,has_kids,stock_options,email)  
select 
    emp_json:employee.name::string as name,
    emp_json:employee.age as age,
    (emp_json:employee.height_in_ft)*(12*2.54) as height_in_cm,
    emp_json:employee.married as is_married,
    emp_json:employee.has_kids as has_kids,
    emp_json:employee.stock_options as stock_options,
    emp_json:employee.email::string as email
from employee_tbl; 

select * from employee_master;

--Insert into employee phone table
insert into emp_phones (emp_fk,phone_type,phone_number)
select 
    b.emp_pk,
    'home_phone' as home_phone,
    a.emp_json:employee.phone[0]::string as home_phone
from 
    employee_tbl a 
    join 
    employee_master b 
    on  
        a.emp_json:employee.email = b.email
union all
select 
    b.emp_pk,
    'work_phone' as work_phone,
    a.emp_json:employee.phone[1]::string as work_phone
from 
    employee_tbl a 
    join 
    employee_master b 
    on  
        a.emp_json:employee.email = b.email;
        
        select * from emp_phones;

--Insert into employee phone --table
insert into emp_address (emp_fk,street_address,city,state)
select 
    b.emp_pk,
    a.emp_json:employee.Address.street::string as street,
    a.emp_json:employee.Address.city::string as city,
    a.emp_json:employee.Address.State::string as state
from 
    employee_tbl a 
    join 
    employee_master b 
    on  
        a.emp_json:employee.email = b.email;
        
select * from emp_address;

select e.*, a.* 
    from employee_master e join emp_address a on e.emp_pk = a.emp_fk;
    


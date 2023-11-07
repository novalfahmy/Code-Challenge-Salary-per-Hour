# Code Challenge: Salary per Hour


## Project Overview
This repository contains a set of SQL and Python scripts to calculate the salary per hour for each branch based on the number of employees and their total working hours each month. The results of this project will help the analyst determine the cost-effectiveness of the current payroll scheme, specifically on a per-hour basis for each branch. The input data is from 
- employees.csv: consists of all-time employee information
- timesheets.csv: consists of daily clock-ins and clock-outs of the employees.

Both files will be transformed and the output data will be stored in a table for easy retrieval by a BI tool using a simple SQL query.



## Data Flow Overview
Because the SQL script is supposed to read the whole table and then overwrite the result in the destination table, the SQL script will be

**SQL Script:**
```plaintext
Truncate Destination Table --> Transform Raw Data --> Insert Into Destination Table
```
Because the Python script is supposed to read the new data and then append the result to the destination table, the Python script will be

**Python Script:** 
```
Read CSV Data --> Transform Raw Data --> Connect to Database --> Check The New Data in Destination Table --> Update or Append New Data 
```


## Getting Started
Before running this script, make sure that you have met the prerequisites and have downloaded this repository


**Prerequisites**
- Python (My version: 3.9.13)
- pip (My version: 22.0.4)
- PostgreSQL 16.0, compiled by Visual C++ build 1935, 64-bit


**Clone Repository** 
```bash 
git clone https://github.com/novalfahmy/Code-Challenge-Salary-per-Hour
```

## SQL Preparation

**Set up required schemas and tables in PostgreSQL**
- **Schema**: there are 2 schemas, staging and analytics. The staging schema is for storing the raw data (employees and timesheets), meanwhile the analytics schema is the output schema for BI queries
- **Table**: there are 3 tables, employees and timesheets tables in staging schema for storing raw employees and timesheets data and salary_per_hour table in analytics schema for storing transformed data ready to be queried by BI tools. 

```sql
-- Scehma
create schema if not exists staging 
;
create schema if not exists analytics 
;

-- Tables
create table if not exists staging.employees (
    employe_id integer,
    branch_id integer,
    salary numeric,
    join_date date,
    resign_date date
)
;
create table if not exists staging.timesheets (
    timesheet_id integer,
    employee_id integer,
    date date,
    checkin time,
    checkout time
)
;
create table if not exists analytics.salary_per_hour (
	year numeric ,
	month numeric ,
	branch_id  integer,
	salary_per_hour numeric(18,4)
)
;
```


**Copy the raw CSV data to staging.employees and staging.timesheets tables using psql CLI** 

1. Activate your psql
```bash
/{psql-path}/psql -U your-username -d your-database -h your-host -p your-port
```

2. Configure your CSV data directory and copy the data
```bash
\copy staging.employees(employe_id, branch_id, salary, join_date, resign_date) from '{path}\employees.csv' with delimiter ',' csv header;
\copy staging.timesheets(timesheet_id, employee_id, date, checkin, checkout) from '{path}\timesheets.csv' with delimiter ',' csv header;
```



## Python Preparation

**Install dependencies from requirements.txt**
```bash 
pip install -r requirements.txt
```


**Check the installed dependencies**
```bash
pip freeze
```

```plaintext 
greenlet==3.0.1
numpy==1.26.1
pandas==2.1.2
psycopg2==2.9.9
python-dateutil==2.8.2
pytz==2023.3.post1
six==1.16.0
SQLAlchemy==2.0.23
typing_extensions==4.8.0
tzdata==2023.3
```


**Set up database configuration, destination schema, and destination table in Python config_db.py**
```python 
db_params = {
    'database': '{your-database}',
    'user': '{your-user}',
    'password': '{your-password}',
    'host': '{your-host}',
    'port': {your-port}
}
schema_name = '{your-destination-schema}'
table_name = '{your-destination-table}'
```



## Usage
To run this script you can execute the `salary_per_hour.py` (Python) or `salary_per_hour.sql` (SQL)



## SQL Script Logic 
**Flow** 
1. Truncate the destination table
2. Transform raw data 
3. Insert the transformed data into the destination table 


**1. Truncate the destination table**
```sql
truncate table analytics.salary_per_hour 
;
```


**2. Transform data**

There are 4 steps with 3 temporary tables needed for transforming the data. I created these 3 temporary tables just to make the code more readable. 

A. cleanse_timesheets_t

This table is to get the absence year and absence month and also cleanse the checkin and checkout data.   

```sql
with cleanse_timesheets_t as (
select
	timesheet_id , 
	employee_id ,
	"date" ,
	extract(year from date) as absence_year ,
	extract(month from date) as absence_month,
	nullif(replace(checkin,'"', ''),'')::time as checkin ,
	nullif(replace(checkout,'"', ''), '')::time as checkout 
from
	staging.timesheets
)
```

B. total_hour_t
   
This table is to remove the employee in a specific year and month if their check time data is invalid, such as checkin time > checkout time, checkin data is null, and checkout data is null. Then, we will sum the total working hours of each employee, absence year and absence month. 

Notes:
The reason I remove employee_id in specific year and month if there is invalid data is because 
- No other data that can replace the invalid data
- The output of the analysis is to define the salary per hour, so I think the accuracy is more important than filling the vague data with something like average over partition of each employee_id and year_month or any other methods

```sql
, total_hour_t as (
select 
	employee_id,
	absence_year ,
	absence_month ,
	sum(extract(epoch from(checkout - checkin)) / 3600) as total_hour 
from 
	cleanse_timesheets_t 
where 
	concat(employee_id, absence_year, absence_month) not in (
		select 
			concat(employee_id, absence_year, absence_month) 
		from 
			cleanse_timesheets_t 
		where checkin > checkout or checkout is null or checkin is null 
		) -- remove employee_id in specific year and month if there is vague data; checkin > checkout or null value in either checkin or checkout
group by 
	1,
	2,
	3
)
```

C. cleanse_employees_t
   
This table is to get the join year and month and also resign year and month. Additionally, this table is also for excluding the duplicate employee_id. 

Notes:
The reason I exclude the duplicate employe_id is because 
- I don't know which one is correct (that employee_id has a different salary while having the same attribute)
- Since the output is salary per hour per batch, not salary per hour per employee, I just want to keep the accuracy by excluding the duplicate value instead of manipulating with avg/min/max or any other methods

```sql
, cleanse_employees_t as (
select 
	employe_id ,
	branch_id , 
	extract(year from nullif(join_date,'')::date) as join_year ,
	extract(month from nullif(join_date,'')::date) as join_month,		
	extract(year from nullif(resign_date,'')::date) as resign_year ,
	extract(month from nullif(resign_date,'')::date) as resign_month,	
	salary 
from 
	staging.employees
where 
	employe_id not in (select employe_id from staging.employees group by 1 having count(employe_id) > 1) -- duplicate employee_id (218078)
)
```

D. Final transformed output
   
This is the final output of the transformed data that consists of year, month, branch_id, and salary_per_hour. I chose inner join between total_hour_t and cleanse_employees_t  because there are filters from those tables that should be applied in the final transformed data. In this final transformed output, I also exclude the working hours of the employees in their first and last month of work.

Notes:
The reason I exclude the working hours of the employees in their first and last month of work is because
- It can make the salarey_per_hour more biased since the output is per month. For instance, the salary_per_hour will be less accurate if there is an employee who just joined on the late dates of the month (example: 2021-03-29) and resigned on the early dates of the month (example: 2022-08-02)

```sql
select 
	a.absence_year as year,
	a.absence_month as month ,
	b.branch_id ,
	sum(b.salary)  / sum(a.total_hour) as salary_per_hour 
from 
	total_hour_t a
inner join 
	cleanse_employees_t b
on 
	a.employee_id = b.employe_id 
where 
	concat(a.absence_year, a.absence_month) != concat(b.resign_year, b.resign_month) -- exclude the working hours of the employees in their last month of work
	and concat(a.absence_year, a.absence_month) != concat(b.join_year, b.join_month) -- exclude the working hours of the employees in their first month of work
group by 
	1,
	2,
	3
```


**3. Insert data to destination table**
```sql
insert into analytics.salary_per_hour (
	year ,
	month ,
	branch_id ,
	salary_per_hour
)
```



## Python Script Logic  

**Flow**
1. Read CSV Data 
2. Transform Raw Data 
3. Connect to Database 
4. Check The Transformed Data in The Destination Table 
5. Update If The Transformed Data Exists or Append The Transformed Data If It Doesn't Exist

There are 4 scripts needed to run this ETL.
- `config_db.py`: this module stores database connection parameters.
- `connect_db.py`: this module connects to the database and loads the transformed data into the destination table.
- `transform_csv.py`: this module handles the transformation of raw data, applying various data cleaning and formatting operations.
- `salary_per_hour.py`: this is the main module that orchestrates the ETL process, importing _config_db.py_, _connect_db.py_, and _transform_csv.py_ to execute the workflow.


### `config_db.py`

This Python script is to set the database configuration, destination schema, and destination table. You can change the {your-xxx} with your database parameters, schema, and table name. 

```python 
db_params = {
    'database': '{your-database}',
    'user': '{your-user}',
    'password': '{your-password}!',
    'host': '{your-host}',
    'port': {your-port}
}
schema_name = '{your-destination-schema}'
table_name = '{your-destination-table}'
```


### `connect_db.py`

This Python script consists of a function to connect to the database and execute SQL commands. The function in this script does 4 works

**1. Connect to the database and create SQLAlchemy engine**
```python 
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
```

**2. Create a table if it does not exist** 
```python 
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            year numeric,
            month numeric,
            branch_id integer,
            salary_per_hoour numeric(18,4)
        )
        """
        cursor.execute(create_table_sql)
        connection.commit()
        print(f'Table {schema_name}.{table_name} has been created or already exists.')
```

**3. Loop through the transformed data and check if there's an existing record with the same year, month, and branch_id**
```python 
        for index, row in data.iterrows():
            year, month, branch_id = row['year'], row['month'], row['branch_id']

            existing_record_sql = f'SELECT * FROM {schema_name}.{table_name} WHERE year = {year} AND month = {month} AND branch_id = {branch_id}'
            cursor.execute(existing_record_sql)
            existing_record = cursor.fetchone()
``` 

**4. Update or append the transformed data to destination table**
```python 
           if existing_record:
                # Update the existing data
                update_sql = f'UPDATE {schema_name}.{table_name} SET salary_per_hour = {row["salary_per_hour"]} WHERE year = {year} AND month = {month} AND branch_id = {branch_id}'
                cursor.execute(update_sql)
                connection.commit()
            else:
                # Insert a new data
                new_data = row.to_frame().transpose()
                new_data.to_sql(table_name, schema=schema_name, if_exists='append', index=False, con=engine)
```

If the script is successful, It will print the successful attempts. If the script is failed, It will print the error. After all, the cursor and the connection will be closed.


### `transform_csv.py`

The _transformation_ logic in the Python script is the same as the transformation logic in the SQL script. This Python script consists of 3 functions to cleanse timesheets data, cleanse employees data, and merge those data. 
- clean_timesheets(timesheets_data)
- clean_employees(employees_data)
- merge_data(timesheets_data, employees_data)



**A. Function: clean_timesheets(timesheets_data)**
   
This function is to cleanse the timesheets data. This function does 5 works

**1. Convert the checkin and checkout**
```python 
        timesheets_data['checkin'] = pd.to_datetime(timesheets_data['checkin'], format='%H:%M:%S', errors='coerce')
        timesheets_data['checkout'] = pd.to_datetime(timesheets_data['checkout'], format='%H:%M:%S', errors='coerce')
```

**2. Create a new column of absence_year and absence_month**
```python 
        timesheets_data['absence_year'] = pd.to_datetime(timesheets_data['date']).dt.year
        timesheets_data['absence_month'] = pd.to_datetime(timesheets_data['date']).dt.month
```

**3. Calculate the leadtime**  
```python 
        timesheets_data['leadtime'] = (timesheets_data['checkout'] - timesheets_data['checkin']).dt.total_seconds() / 3600
```

**4. Cleanse the invalid data (checkin > checkout, checkin is NA, checkout is NA)**
```python 
        removed_data = timesheets_data[(timesheets_data['checkin'] > timesheets_data['checkout']) | (pd.isna(timesheets_data['checkin'])) | (pd.isna(timesheets_data['checkout']))].copy()
        removed_data['concat_col'] = removed_data['employee_id'].astype(str) + removed_data['absence_year'].astype(str) + removed_data['absence_month'].astype(str)
        timesheets_data['concat_col'] = timesheets_data['employee_id'].astype(str) + timesheets_data['absence_year'].astype(str) + timesheets_data['absence_month'].astype(str)
        timesheets_data = timesheets_data[~timesheets_data['concat_col'].isin(removed_data['concat_col'])]
```

**5. Set the columns and sum the leadtime in each employee_id, absence_year, and absence_month of the final timesheets data**
```python 
        timesheets_col = ['employee_id', 'absence_year', 'absence_month', 'leadtime']
        final_timesheets_data = timesheets_data[timesheets_col]
        final_timesheets_data = final_timesheets_data.groupby(['employee_id', 'absence_year', 'absence_month'])['leadtime'].sum().reset_index()
        return final_timesheets_data
```


**B. Function: clean_employees(employees_data)**
   
This function is to cleanse the employees data. This function does 3 works

**1. Create columns of join year and month and resign year and month**
```python 
        employees_data['join_year'] = pd.to_datetime(employees_data['join_date'], errors='coerce').dt.year.astype('Int64')
        employees_data['join_month'] = pd.to_datetime(employees_data['join_date'], errors='coerce').dt.month.astype('Int64')
        employees_data['resign_year'] = pd.to_datetime(employees_data['resign_date'], errors='coerce').dt.year.astype('Int64')
        employees_data['resign_month'] = pd.to_datetime(employees_data['resign_date'], errors='coerce').dt.month.astype('Int64')
```

**2. Create the concat columns of join year month and resign year month**
```python 
        employees_data['join_year_month'] = np.where(employees_data['join_date'].isnull(), np.nan, employees_data['join_year'].astype(str) + employees_data['join_month'].astype(str)) 
        employees_data['resign_year_month'] = np.where(employees_data['resign_date'].isnull(), np.nan, employees_data['resign_year'].astype(str) + employees_data['resign_month'].astype(str))
```

**3. Drop duplicates in employe_id and set the columns of the final employees data**
```python 
        final_employees_data = employees_data.drop_duplicates(subset='employe_id', keep=False)
        employees_col = ['employe_id', 'branch_id', 'join_year_month', 'resign_year_month', 'salary']
        final_employees_data = final_employees_data[employees_col]
        return final_employees_data
```


**C. Function: merge_data(timesheets_data, employees_data)**
   
This function is to merge the final timesheets data and the final employees data. This function does 4 works

**1. Merge the timesheets and employees tables**
```python 
        merged_data = pd.merge(timesheets_data, employees_data, left_on='employee_id', right_on='employe_id', how='inner')
```

**2. Exclude the working hours of the employees in their first and last month of work.**
```python 
        merged_data['absence_year_month'] = merged_data['absence_year'].astype(str) + merged_data['absence_month'].astype(str)
        exc_cond = ((merged_data['absence_year_month'] != merged_data['join_year_month']) & (merged_data['absence_year_month'] != merged_data['resign_year_month']))
        merged_data = merged_data[exc_cond]
```

**3. Calculate the salary_per_hour** 
```python 
        merged_data = merged_data[['absence_year', 'absence_month', 'branch_id', 'salary', 'leadtime']].groupby(['absence_year', 'absence_month', 'branch_id']).agg({'salary': 'sum', 'leadtime': 'sum'}).reset_index()
        merged_data['salary_per_hour'] = (merged_data['salary'] / merged_data['leadtime']).round(4)
```

**4. Set the columns of the final merged data**
```python 
        final_merged_data = merged_data[['absence_year', 'absence_month', 'branch_id', 'salary_per_hour']]
        final_merged_data = final_merged_data.rename(columns={'absence_year': 'year', 'absence_month': 'month'})
        return final_merged_data
```

If the script is successful, It will print the successful attempts. If the script is failed, It will print the error of each function.


### `salary_per_hour.py`

This Python script consists of a function to import and execute all modules (_config_db.py_, _connect_db.py_, _transform_csv.py_).
. 
This function does 4 works

**1. Set the CSV path**
```python 
        employees_csv_path = 'data/employees.csv'
        timesheets_csv_path = 'data/timesheets.csv'
```

**2. Read the timesheets and employees CSV and execute the cleansing function if the data is not empty**
```python 
        # Timesheets
        timesheets_data = pd.read_csv(timesheets_csv_path)
        if not timesheets_data.empty:
            timesheets_data = clean_timesheets(timesheets_data)
        else:
            raise ValueError("Timesheets data is empty or invalid.")

        # Employees
        employees_data = pd.read_csv(employees_csv_path)
        if not employees_data.empty:
            employees_data = clean_employees(employees_data)
        else:
            raise ValueError("Employees data is empty or invalid.")
```

**3. Check the timesheets and employees data and merge those data if they are not empty**
```python 
        if timesheets_data is not None and employees_data is not None:
            final_data = merge_data(timesheets_data, employees_data)
        else:
            raise ValueError("Data merging failed due to invalid input.")
```

**4. Load the merged data to the destination table if it is not empty**
```python 
        if final_data is not None:
            load_func = load_data(final_data, db_params, schema_name, table_name)
            return load_func
        else:
            raise ValueError("Data can't be loaded to destination table due to invalid input")
```
If the script is successful, It will print the successful attempts. If the script is failed, It will print the error.



## Project Limitations 
There are limitations in this project
- This script doesn't include the scheduler function
- The database used is a public database, so it has no function to configure SSH tunnel 

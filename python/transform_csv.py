import pandas as pd
import numpy as np

def clean_timesheets(timesheets_data):
    try:
        # Convert the checkin and checkout to be datetime format
        timesheets_data['checkin'] = pd.to_datetime(timesheets_data['checkin'], format='%H:%M:%S', errors='coerce')
        timesheets_data['checkout'] = pd.to_datetime(timesheets_data['checkout'], format='%H:%M:%S', errors='coerce')

        # Create new col absence_year and absence_month
        timesheets_data['absence_year'] = pd.to_datetime(timesheets_data['date']).dt.year
        timesheets_data['absence_month'] = pd.to_datetime(timesheets_data['date']).dt.month

        # Calculate the leadtime
        timesheets_data['leadtime'] = (timesheets_data['checkout'] - timesheets_data['checkin']).dt.total_seconds() / 3600

        # Cleanse the invalid data
        removed_data = timesheets_data[(timesheets_data['checkin'] > timesheets_data['checkout']) | (pd.isna(timesheets_data['checkin'])) | (pd.isna(timesheets_data['checkout']))].copy()
        removed_data['concat_col'] = removed_data['employee_id'].astype(str) + removed_data['absence_year'].astype(str) + removed_data['absence_month'].astype(str)
        timesheets_data['concat_col'] = timesheets_data['employee_id'].astype(str) + timesheets_data['absence_year'].astype(str) + timesheets_data['absence_month'].astype(str)
        timesheets_data = timesheets_data[~timesheets_data['concat_col'].isin(removed_data['concat_col'])]

        # Finalize the timesheets table
        timesheets_col = ['employee_id', 'absence_year', 'absence_month', 'leadtime']
        final_timesheets_data = timesheets_data[timesheets_col]
        final_timesheets_data = final_timesheets_data.groupby(['employee_id', 'absence_year', 'absence_month'])['leadtime'].sum().reset_index()
        return final_timesheets_data
    except Exception as e:
        print(f"An error occurred during data cleaning in clean_timesheets: {str(e)}")
        return None

def clean_employees(employees_data):
    try:
        # Create join and resign year and month columns
        employees_data['join_year'] = pd.to_datetime(employees_data['join_date'], errors='coerce').dt.year.astype('Int64')
        employees_data['join_month'] = pd.to_datetime(employees_data['join_date'], errors='coerce').dt.month.astype('Int64')
        employees_data['resign_year'] = pd.to_datetime(employees_data['resign_date'], errors='coerce').dt.year.astype('Int64')
        employees_data['resign_month'] = pd.to_datetime(employees_data['resign_date'], errors='coerce').dt.month.astype('Int64')

        # Create the concat columns of join year month and resign year month
        employees_data['join_year_month'] = np.where(employees_data['join_date'].isnull(), np.nan, employees_data['join_year'].astype(str) + employees_data['join_month'].astype(str)) 
        employees_data['resign_year_month'] = np.where(employees_data['resign_date'].isnull(), np.nan, employees_data['resign_year'].astype(str) + employees_data['resign_month'].astype(str))
        
        # Finalize the employees table
        final_employees_data = employees_data.drop_duplicates(subset='employe_id', keep=False)
        employees_col = ['employe_id', 'branch_id', 'join_year_month', 'resign_year_month', 'salary']
        final_employees_data = final_employees_data[employees_col]
        return final_employees_data
    except Exception as e:
        print(f"An error occured during data cleaning in clean_employees: {str(e)}")
        return None

def merge_data(timesheets_data, employees_data):
    try: 
        # Merge the timesheets and employees tables
        merged_data = pd.merge(timesheets_data, employees_data, left_on='employee_id', right_on='employe_id', how='inner')

        # Exclude the employee_id in the specific year and month that is in range of join year month and resign year month 
        merged_data['absence_year_month'] = merged_data['absence_year'].astype(str) + merged_data['absence_month'].astype(str)
        exc_cond = ((merged_data['absence_year_month'] != merged_data['join_year_month']) & (merged_data['absence_year_month'] != merged_data['resign_year_month']))
        merged_data = merged_data[exc_cond]

        # Sum the salary per hour
        merged_data = merged_data[['absence_year', 'absence_month', 'branch_id', 'salary', 'leadtime']].groupby(['absence_year', 'absence_month', 'branch_id']).agg({'salary': 'sum', 'leadtime': 'sum'}).reset_index()
        merged_data['salary_per_hour'] = (merged_data['salary'] / merged_data['leadtime']).round(4)

        # Finalize the table
        final_merged_data = merged_data[['absence_year', 'absence_month', 'branch_id', 'salary_per_hour']]
        final_merged_data = final_merged_data.rename(columns={'absence_year': 'year', 'absence_month': 'month'})
        return final_merged_data
    except Exception as e:
        print(f"An error occured during data merging: {str(e)}")
        return None
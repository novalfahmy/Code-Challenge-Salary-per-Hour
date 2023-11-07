import pandas as pd
from transform_csv import clean_employees, clean_timesheets, merge_data
from config_db import db_params, schema_name, table_name
from connect_db import load_data

def main():
    try:
        # Set CSV path
        employees_csv_path = 'data/employees.csv'
        timesheets_csv_path = 'data/timesheets.csv'

        # Read the timesheets CSV and execute the cleanse_timesheets function if the data is not empty
        timesheets_data = pd.read_csv(timesheets_csv_path)
        if not timesheets_data.empty:
            timesheets_data = clean_timesheets(timesheets_data)
        else:
            raise ValueError("Timesheets data is empty or invalid.")

        # Read the employees CSV and execute the cleanse_employees function if the data is not empty
        employees_data = pd.read_csv(employees_csv_path)
        if not employees_data.empty:
            employees_data = clean_employees(employees_data)
        else:
            raise ValueError("Employees data is empty or invalid.")

        # Merge the timesheets and employees data by running the merge_data function if both data is not empty 
        if timesheets_data is not None and employees_data is not None:
            final_data = merge_data(timesheets_data, employees_data)
        else:
            raise ValueError("Data merging failed due to invalid input.")
        
        # Load data to destination table if both data input is valid
        if final_data is not None:
            load_func = load_data(final_data, db_params, schema_name, table_name)
            return load_func
        else:
            raise ValueError("Data can't be loaded to destination table due to invalid input")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == '__main__':
    main()
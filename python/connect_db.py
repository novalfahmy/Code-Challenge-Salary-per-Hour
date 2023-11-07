import psycopg2
from sqlalchemy import create_engine

def load_data(data, db_params, schema_name, table_name):
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)

        # Create a database cursor from the connection
        cursor = connection.cursor()

        # Create a SQLAlchemy engine using the connection
        engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')        

        # Create the table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            year numeric,
            month numeric,
            branch_id integer,
            salary_per_hour numeric(18,4)
        )
        """
        cursor.execute(create_table_sql)
        connection.commit()
        print(f'Table {schema_name}.{table_name} has been created or already exists.')

        # Loop through the transformed data to determine whether to append or update
        for index, row in data.iterrows():
            year, month, branch_id = row['year'], row['month'], row['branch_id']

            # Check if there's an existing record with the same year, month, and branch_id
            existing_record_sql = f'SELECT * FROM {schema_name}.{table_name} WHERE year = {year} AND month = {month} AND branch_id = {branch_id}'
            cursor.execute(existing_record_sql)
            existing_record = cursor.fetchone()

            if existing_record:
                # Update the existing data
                update_sql = f'UPDATE {schema_name}.{table_name} SET salary_per_hour = {row["salary_per_hour"]} WHERE year = {year} AND month = {month} AND branch_id = {branch_id}'
                cursor.execute(update_sql)
                connection.commit()
            else:
                # Insert a new data
                new_data = row.to_frame().transpose()
                new_data.to_sql(table_name, schema=schema_name, if_exists='append', index=False, con=engine)

        print('Data has been loaded.')
    except Exception as e:
        print(f'An error occurred: {str(e)}')
    finally:
        # Close the cursor and the connection in a finally block to ensure they are always closed
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
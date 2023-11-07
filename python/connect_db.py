import psycopg2
from sqlalchemy import create_engine

def load_data(data, db_params, schema_name, table_name):
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)

        # Create a database cursor from the connection
        cursor = connection.cursor()

        # Create the table if it doesn't exist
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

        # Truncate table 
        truncate_sql = f'TRUNCATE TABLE {schema_name}.{table_name}'
        cursor.execute(truncate_sql)
        connection.commit()
        print(f'Data in {schema_name}.{table_name} has been truncated.')

        # Create a SQLAlchemy engine using the connection
        engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

        # Load the data to database schema and table 
        data.to_sql(table_name, schema=schema_name, if_exists='replace', index=False, con=engine)
        print(f'Data has been loaded into {schema_name}.{table_name}.')
    except Exception as e:
        print(f'An error occurred: {str(e)}')
    finally:
        # Close the cursor and the connection in a finally block to ensure they are always closed
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    
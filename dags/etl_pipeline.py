from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Extract and Transform coffee shop orders',
    schedule_interval=None,
)


def extract(ti, **kwargs):
    csv_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'coffee_shop_orders.csv')
    df = pd.read_csv(csv_file_path)

    # Convert datetime columns to strings
    if 'order_time' in df.columns:
        df['order_time'] = df['order_time'].astype(str)

    # Convert DataFrame to dictionary and push to XCom
    xcom_value = df.to_dict(orient='list')
    ti.xcom_push(key='extracted_data', value=xcom_value)
    print(f"Pushed to XCom: {len(xcom_value)} columns, {len(xcom_value[list(xcom_value.keys())[0]])} rows")


def transform(ti, **kwargs):
    # Pull data from XCom
    df_dict = ti.xcom_pull(task_ids='extract_task', key='extracted_data')
    print(f"Pulled DataFrame Dict: {type(df_dict)}")
    print(f"Dict keys: {df_dict.keys() if df_dict else 'None'}")
    print(f"Dict values length: {len(df_dict[list(df_dict.keys())[0]]) if df_dict else 'None'}")

    if df_dict is None or not df_dict:
        raise ValueError("No data was pulled from XCom. Please check the extract task.")

    # Convert dictionary back to DataFrame
    df = pd.DataFrame(df_dict)

    if 'order_time' in df.columns:
        df['order_time'] = pd.to_datetime(df['order_time'])
        df['order_date'] = df['order_time'].dt.date
        df['order_Time'] = df['order_time'].dt.strftime('%H:%M')
        df = df.drop(columns=['order_time'])

    # Add 'total_price' column
    if 'quantity' in df.columns and 'price' in df.columns:
        df['total_price'] = df['quantity'] * df['price']
        df['total_price'] = df['total_price'].astype(float)
        print(f"Transformed DataFrame after processing: {df.head()}")

        # Convert the DataFrame to a dictionary, handling Timestamp objects
        df_dict = df.to_dict(orient='records')

        ti.xcom_push(key='transformed_data', value=df_dict)
    else:
        raise KeyError("'quantity' or 'price' column is missing in the DataFrame")


def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='transformed_data', task_ids='transform_task')

    if not df_dict:
        raise ValueError("No data retrieved from XCom")

    df = pd.DataFrame(df_dict)

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

    create_orders_table_query = '''
    CREATE TABLE IF NOT EXISTS coffee_shop_orders (
        order_id INT PRIMARY KEY,
        customer_name VARCHAR(255),
        drink VARCHAR(50),
        quantity INT,
        price FLOAT,
        total_price FLOAT,
        order_date DATE,
        order_Time TIME
    );
    '''

    create_menu_table_query = '''
    CREATE TABLE IF NOT EXISTS menu_items (
        drink VARCHAR(50) PRIMARY KEY,
        price FLOAT
    );
    '''

    try:
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Create orders table
                cursor.execute(create_orders_table_query)
                conn.commit()

                orders_data = [(
                    row['order_id'],
                    row['customer_name'],
                    row['drink'],
                    row['quantity'],
                    row['price'],
                    row['total_price'],
                    row['order_date'],
                    row['order_Time']
                ) for _, row in df.iterrows()]

                # Bulk insert orders data
                cursor.executemany(
                    "INSERT INTO coffee_shop_orders (order_id, customer_name, drink, quantity, price, total_price, order_date, order_Time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    orders_data
                )
                conn.commit()

                # Create menu items table
                cursor.execute(create_menu_table_query)
                conn.commit()

                # Insert unique drinks and their prices
                unique_drinks = df[['drink', 'price']].drop_duplicates().to_records(index=False)
                cursor.executemany(
                    "INSERT INTO menu_items (drink, price) VALUES (%s, %s) ON CONFLICT (drink) DO NOTHING",
                    unique_drinks
                )
                conn.commit()
        print(f"Successfully inserted {len(orders_data)} rows into coffee_shop_orders table")
    except Exception as e:
        print(f"Error occurred while inserting data: {str(e)}")
        raise


extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres_task',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_to_postgres_task

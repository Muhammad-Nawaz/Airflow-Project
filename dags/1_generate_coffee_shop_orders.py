from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
from faker import Faker
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'generate_coffee_shop_orders',
    default_args=default_args,
    description='Generate coffee shop orders and save to CSV',
    schedule_interval=None,
)

def generate_orders(**kwargs):
    fake = Faker()

    # Define the directory and file path
    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)
    csv_file_path = os.path.join(data_dir, "coffee_shop_orders.csv")

    # Check if the file already exists
    if os.path.exists(csv_file_path):
        print(f"File {csv_file_path} already exists. Skipping order generation.")
        return

    # Define columns
    columns = ["order_id", "customer_name", "drink", "quantity", "price", "order_time"]

    # Create a list of drinks and their prices
    drinks = {
        "Espresso": 2.5,
        "Latte": 3.5,
        "Cappuccino": 3.0,
        "Americano": 2.0,
        "Mocha": 4.0,
        "Flat White": 3.75,
        "Macchiato": 3.25,
        "Iced Coffee": 3.0,
        "Cold Brew": 3.5,
        "Frappuccino": 4.5,
        "Tea": 2.0,
        "Hot Chocolate": 3.0
    }

    # Generate random orders
    data = []
    base_date = datetime(2023, 5, 1, 0, 0)
    for i in range(100):  # Generate 100 orders
        drink = random.choice(list(drinks.keys()))
        order_time = base_date + timedelta(minutes=i * (60*24)//100)
        data.append([
            i + 1,
            fake.name(),
            drink,
            random.randint(1, 3),  # Quantity between 1 and 3
            drinks[drink],
            order_time
        ])

    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)

    # Save to CSV
    df.to_csv(csv_file_path, index=False)
    print(f"Data saved to {csv_file_path}")

generate_orders_task = PythonOperator(
    task_id='generate_orders_task',
    python_callable=generate_orders,
    provide_context=True,
    dag=dag,
)

generate_orders_task
# Airflow ETL Project

## Introduction
This project is designed to simulate an ETL (Extract, Transform, Load) pipeline for a coffee shop's orders. The pipeline will generate synthetic coffee shop order data, transform it, and load it into a PostgreSQL database. The project uses Apache Airflow for managing the workflow and PostgreSQL for data storage.

## Packages Required
To run this project, you will need the following:

1. **Suitable IDE**: 
   - Visual Studio Code or PyCharm are recommended for this project.
   
2. **Docker**:
   - Docker installed and running.
   - Ensure at least 5GB of memory is available for containers in the Docker settings.

## How to Run This Project

1. **Clone the Project**:
   - Clone this project into a local directory on your machine using the following command:
     ```bash
     git clone <repository-url>
     ```
   - Navigate into the project directory:
     ```bash
     cd <project-directory>
     ```

2. **Install Required Python Packages**:
   - Run this command in your terminal to install the required Python packages:
     ```bash
     pip install -r requirements.txt
     ```

3. **Start Docker Containers**:
   - Run the Docker Compose file to start up the Airflow and PostgreSQL containers using this command:
     ```bash
     docker compose up -d
     ```
   - Wait for all the containers to finish building. You will have a total of 8 containers with 1 saying "exited," which is normal.

4. **Access Airflow UI**:
   - Travel to [localhost:8080](http://localhost:8080) and sign in with the username and password both being `airflow`.

5. **Configure PostgreSQL Connection in Airflow**:
   - In the Airflow UI, go to **Admin** > **Connections**.
   - Add a new connection with the following parameters:
     - **Conn Id**: `postgres_default`
     - **Conn Type**: `Postgres`
     - **Host**: `postgres`
     - **Schema**: `airflow`
     - **Login**: `airflow`
     - **Password**: `airflow`
     - **Port**: `5432`
   - Click **Save**.

6. **Configure pgAdmin**:
   - Go to [localhost:5050](http://localhost:5050) to access pgAdmin.
   - Click on the **Add a new server** button with the following configurations:
     - **General tab**:
       - **Name**: `Airflow Postgres`
     - **Connection tab**:
       - **Host name/address**: `postgres`
       - **Port**: `5432`
       - **Maintenance database**: `airflow`
       - **Username**: `airflow`
       - **Password**: `airflow`
   - Click **Save**.

7. **Run the DAGs**:
   - In the Airflow UI, trigger the `generate_coffee_shop_orders` DAG. After it finishes running, you will see a CSV file in the `data` folder of your Airflow project directory.
   - Next, trigger the `etl_pipeline` DAG and wait for it to finish running.

8. **View Transformed Data in pgAdmin**:
   - Go back to pgAdmin at [localhost:5050](http://localhost:5050).
   - On the left-hand side, find the server we created earlier called `Airflow Postgres`.
   - Click inside and follow this path: `Airflow Postgres > Databases > airflow > Schemas > Public > Tables`.
   - Right-click the `Tables` heading and click **Refresh**. You should now be able to see the two new tables: `coffee_shop_orders` and `menu_items`.
   - To see the data, right-click any of the tables and then click **View/Edit Data**. You should be able to see all the data our pipeline outputted into PostgreSQL.
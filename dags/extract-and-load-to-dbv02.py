import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

# DAG Define
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'data_pipelinev02',
    default_args=default_args,
    description='A DAG to load, clean, and insert data into SQL database on Azure',
    schedule_interval='0 12 * * *',  
    catchup=False,
)

def extract_and_transform_data(**kwargs):
    url = "https://raw.githubusercontent.com/Kalinoszczak/Data/main/CSV/cars.csv"
    df = pd.read_csv(url, sep=',')
    # Changing the column name to Polish
    df.columns = ["Index", "Price", "Year", "Mileage", "Power", "Engine capacity", "Doors"]
    # Clearing data
    df = df.dropna()
    # Transformation process
    df_filtered = df[df['Doors'] == 4.0]
    df_sorted = df_filtered.sort_values(by='Year', ascending=False)
    q1 = df_sorted['Price'].quantile(0.25)
    q3 = df_sorted['Price'].quantile(0.75)

    def categorize_price(Price):
        if Price <= q1:
            return 'cheap'
        elif Price > q1 and Price <= q3:
            return 'average'
        else:
            return 'expensive'

    df_sorted['Price Category'] = df_sorted['Price'].apply(categorize_price)
    df_sorted['Price/Power'] = df_sorted['Price'] / df_sorted['Power']
    df_sorted['Price/Engine capacity'] = df_sorted['Price'] / df_sorted['Engine capacity']
    
    # Data transfer with XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=df_sorted.to_dict())

def get_db_connection_string():
    conn = BaseHook.get_connection('azure_sql_server')
    conn_str = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}?driver=ODBC+Driver+18+for+SQL+Server"
    return conn_str

def create_table_if_not_exists():
    conn_str = get_db_connection_string()
    engine = create_engine(conn_str)
    with engine.connect() as connection:
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='transformed_data' AND xtype='U')
        CREATE TABLE transformed_data (
            [Index] INT PRIMARY KEY,
            [Price] FLOAT,
            [Year] INT,
            [Mileage] FLOAT,
            [Power] FLOAT,
            [Engine capacity] FLOAT,
            [Doors] FLOAT,
            [Price Category] VARCHAR(50),
            [Price/Power] FLOAT,
            [Price/Engine capacity] FLOAT
        );
        """
        connection.execute(text(create_table_query))

def clear_table():
    conn_str = get_db_connection_string()
    engine = create_engine(conn_str)
    with engine.connect() as connection:
        clear_table_query = "DELETE TABLE transformed_data;"
        connection.execute(text(clear_table_query))

def load_data_to_sql(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='extract_and_transform_data', key='transformed_data')
    df = pd.DataFrame(transformed_data)
    
    conn_str = get_db_connection_string()
    engine = create_engine(conn_str)
    
    with engine.connect() as connection:
        df.to_sql('transformed_data', con=engine, if_exists='append', index=False)

# Defining tasks in DAG
extract_and_transform_task = PythonOperator(
    task_id='extract_and_transform_data',
    python_callable=extract_and_transform_data,
    provide_context=True,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

clear_table_task = PythonOperator(
    task_id='clear_table',
    python_callable=clear_table,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_sql,
    provide_context=True,
    dag=dag,
)

# Defining the order of tasks
extract_and_transform_task >> create_table_task >> clear_table_task >> load_data_task
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import requests
import pandas as pd

def get_data(**kwargs):     
    url = "https://github.com/Kalinoszczak/Data/tree/main/CSV/cars.csv"
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url)

        #convert df to json string from xcom
        json_data = df.to_json(orient="records")
        kwargs["ti"].xcom_push(key="data", value=json_data)
    else:
        raise Exception(f"Failed to get data HTTP status code: {response.status_code}")

def preview_data(**kwargs):
    output_data = kwargs["ti"].xcom_pull(key="data", task_ids="get_data")
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError("No data received from Xcom")    
    #Create dataframe for JSON data

    df = pd.DataFrame(output_data)




def get_datav01(**kwargs):
    url = "https://raw.githubusercontent.com/Kalinoszczak/Data/main/CSV/cars.csv"
    df = pd.read_csv(url, sep=',')

    # converting column names to English
    df.columns = ["Index", "Price", "Year", "Mileage", "Power", "Engine capacity", "Doors"]
    # Cleaning
    df = df.dropna()

    df_filtered = df[df['Doors'] == 4.0]
    df_sorted = df_filtered.sort_values(by='Year', ascending=False)
    
    # Transform process
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
    transformed_data = df_sorted






# Definiowanie domyślnych argumentów dla DAG
default_args = {
    'owner': 'Kalinoszczak',
    'start_date': datetime(2024, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Definiowanie DAG
dag = DAG(
    'fetch',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    catchup=False  # To prevent the DAG from running for past dates
)    

get_data_from_url = PythonOperator (
    task_id = "get_data",
    python_callable = get_data,
    dag = dag
)

preview_data_from_url = PythonOperator (
    task_id = "preview_data",
    python_callable = preview_data,
    dag=dag
)

get_data_from_url >> preview_data_from_url
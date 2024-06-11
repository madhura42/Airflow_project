```python
import pandas as pd
import gzip
from sqlalchemy import create_engine
import sqlite3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
```

Step 1: Data Extraction 

The data extraction involves reading the compressed JSON Lines file and loading it into a DataFrame using pandas.


```python
def extract_data():

    file_path = 'athlete_events_2006_2016.jsonl'

    df = pd.read_json(file_path, lines=True)
    df.to_csv('extracted_data.csv', index=False)
```

Step 2: Data Transformation

Transform the data to obtain the necessary information, such as filtering relevant columns and counting medals.


```python
def transform_data():
    
    df = pd.read_csv('extracted_data.csv')

    df_filtered = df[['year', 'season', 'noc', 'medal']].dropna(subset=['medal'])
    
    medal_counts = df_filtered.groupby(['year', 'season', 'noc']).size().reset_index(name='Medal_Count')
    
    country_counts = df_filtered.groupby(['year', 'season'])['noc'].nunique().reset_index(name='Countries_with_Medals')

    medal_counts.to_csv('transformed_medal_counts.csv', index=False)
    country_counts.to_csv('transformed_country_counts.csv', index=False)

```

Step 3: Data Loading

Create a suitable database and define the table structure and then, insert the transformed data into the database.


```python
def load_data():
    
    medal_counts = pd.read_csv('transformed_medal_counts.csv')
    country_counts = pd.read_csv('transformed_country_counts.csv')
    
    connection = sqlite3.connect('countries_medals.db')
    create_medal_table_query = '''
    CREATE TABLE IF NOT EXISTS MedalCounts (
        year INT,
        season VARCHAR(20),
        noc VARCHAR(3),
        Medal_Count INT,
        PRIMARY KEY (year, season, noc)
    );
    '''
    connection.execute(create_medal_table_query)
    medal_counts.to_sql('MedalCounts', connection, if_exists='replace', index=False)

    create_medal_table_query = '''
    CREATE TABLE IF NOT EXISTS CountryCounts (
        year INT,
        season VARCHAR(20),
        Countries_with_Medals INT,
        PRIMARY KEY (year, season)
    );
    '''
    connection.execute(create_country_table_query)
    country_counts.to_sql('CountryCounts', connection, if_exists='replace', index=False)
    
    connection.commit()
    connection.close()

```

Step 4: Automation and Scheduling

Use tools like Apache Airflow for scheduling the ETL pipeline. Below is an example to schedule dags using Airflow


```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for athlete events data',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
   task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

extract_task >> transform_task >> load_task
```

Step 5: Tools Recommendation

Use tools like Fivetran for the ETL process for a managed service. AWS components like Lambda and S3 can also be used for automation.

Extraction: AWS S3 for file storage.
Transformation: AWS Lambda for processing data.
Loading: AWS Redshift or an RDS database for storing processed data.


```python

```

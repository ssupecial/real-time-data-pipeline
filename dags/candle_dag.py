from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from psycopg2 import sql
import os
from airflow.operators.postgres_operator import PostgresOperator
import io

# S3 버킷 설정
S3_BUCKET = os.environ.get('BUCKET_NAME')
S3_PREFIX = 'candle/'

# PostgreSQL 연결 설정
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost') 
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'postgres')  
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', 5432)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=9)
}

dag = DAG(
    'candle_processing',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

def list_recent_files_from_s3(bucket, prefix, n=5):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_KEY'),
        region_name='ap-northeast-2'
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
    recent_files = [file['Key'] for file in files[:n]]
    return recent_files

def process_parquet_files(**kwargs):
    s3 = boto3.client('s3')
    recent_files = kwargs['ti'].xcom_pull(task_ids='list_recent_files')

    dataframes = []
    for file in recent_files:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=file)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        dataframes.append(df)

    if dataframes:
        combined_df = pd.concat(dataframes)
        result = {
            "batch_start_file_name": recent_files[-1],
            "batch_finish_file_name": recent_files[0],
            "timestamp_min": pd.to_datetime(combined_df['timestamp'].min()).to_pydatetime(),
            "timestamp_max": pd.to_datetime(combined_df['timestamp'].max()).to_pydatetime(),
            "candle_acc_trade_volume_sum": float(combined_df['candle_acc_trade_volume'].sum()),
            "candle_acc_trade_price_sum": float(combined_df['candle_acc_trade_price'].sum()),
            "trade_std": float(combined_df['trade'].std()),
            "trade_min": float(combined_df['trade'].min()),
            "trade_max": float(combined_df['trade'].max()),
            "open_min": float(combined_df['open'].min()),
            "high_max": float(combined_df['high'].max()),
            "low_min": float(combined_df['low'].min()),
        }
        
        # 결과를 XCom으로 전달
        kwargs['ti'].xcom_push(key='process_result', value=result)



    
def save_to_postgres(**kwargs):
    result = kwargs['ti'].xcom_pull(task_ids='process_parquet_files', key='process_result')

    try:
        # PostgreSQL 연결
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )
        cur = conn.cursor()
        
        # SQL 삽입 쿼리 작성
        insert_query = """
            INSERT INTO upbit_candle_result (batch_start_file_name, batch_finish_file_name, timestamp_min, timestamp_max, volume_sum, 
                                     price_sum, trade_std, trade_min, trade_max, 
                                     open_min, high_max, low_min)
            VALUES (%(batch_start_file_name)s, %(batch_finish_file_name)s, %(timestamp_min)s, %(timestamp_max)s, %(candle_acc_trade_volume_sum)s, 
                    %(candle_acc_trade_price_sum)s, %(trade_std)s, %(trade_min)s, %(trade_max)s, 
                    %(open_min)s, %(high_max)s, %(low_min)s)
        """
        
        # 데이터 삽입
        cur.execute(insert_query, result)
        
        # 변경 사항 커밋
        conn.commit()
        
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

list_files_task = PythonOperator(
    task_id='list_recent_files',
    python_callable=list_recent_files_from_s3,
    op_kwargs={'bucket': S3_BUCKET, 'prefix': S3_PREFIX, 'n': 5},
    dag=dag,
)

process_files_task = PythonOperator(
    task_id='process_parquet_files',
    python_callable=process_parquet_files,
    provide_context=True,
    dag=dag,
)

save_to_postgres_task = PythonOperator(
    task_id='save_to_postgres',
    python_callable=save_to_postgres,
    provide_context=True,
    dag=dag,
)



list_files_task >> process_files_task >> save_to_postgres_task


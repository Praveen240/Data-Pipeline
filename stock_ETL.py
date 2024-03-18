from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
import json
from json import loads,dumps
import boto3
from time import sleep

default_args = {
    "owner": "Praveen",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    'start_date': datetime(2024, 2, 20),
    'end_date': datetime(2024, 6, 24)
}

@dag(
    dag_id="Stock_data_etl",
    description="Getting real-time data",
    default_args=default_args,
    schedule_interval='@daily'  
)
def etl_dag():

    def started():
        print("Producer started producing and s3 bucket is consuming data!")
        

    def run_producer():
        producer = KafkaProducer(
            bootstrap_servers=[],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

        stock_data = pd.read_csv("/opt/airflow/data/stock_data_kafka.csv")
        n = 0
        while 1:  
            dict_data = stock_data.sample(1).to_dict(orient="records")[0]
            producer.send("projectD", value=dict_data)
            sleep(2)
            n+=1
            if n==20:
                break

    def run_consumer():
        bootstrap_servers = []
        topic_name = 'projectD'
        group_id = 'Praveen_admin'

        bucket_name = 'stock-kafka-data'
        s3_client = boto3.client('s3', aws_access_key_id, aws_secret_access_key)

        consumer = KafkaConsumer(
            topic_name,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: loads(x.decode('utf-8')) ,
            enable_auto_commit=False       
        )

        c = 0
        for message in consumer:
            try:
                json_data = message.value
                key = f'stock_{c}.json'
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=json.dumps(json_data).encode('utf-8')
                )
                print(f"Uploaded message {c} to S3")
                c += 1
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON message: {e}")
            except Exception as e:
                print(f"Error processing message: {e}")
            if c==20:
                break

        consumer.close()

    def Done():
        print("Task was Done!")


    Start = PythonOperator(
        task_id = 'Start',
        python_callable = started
    )

    Run_Kafka_producer = PythonOperator(
        task_id = 'run_producer',
        python_callable = run_producer
    )

    Run_Kafka_consumer = PythonOperator(
        task_id = 'run_consumer',
        python_callable = run_consumer
    )

    End = PythonOperator(
        task_id = "Completion",
        python_callable = Done
    )

    Start >> Run_Kafka_producer
    Start >> Run_Kafka_consumer
    Run_Kafka_consumer >> End

etl_process = etl_dag()


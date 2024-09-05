import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from kafka import KafkaConsumer
import io
from datetime import datetime
from json import loads
from dotenv import load_dotenv
import os

# S3 연결 설정
s3_client = boto3.client("s3")
bucket_name = os.getenv("BUCKET_NAME")


def save_to_s3(
    data, topic_name
):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    output = io.BytesIO()
    pq.write_table(table, output)
    output.seek(0)
    s3_prefix = f"{topic_name}/"
    s3_key = f"{s3_prefix}{timestamp}.parquet"
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=output.getvalue())


def consume_and_save(topics):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="stream_test",
        value_deserializer=lambda x: loads(x.decode("utf-8")),
        consumer_timeout_ms=10000000,
    )

    data_batch = []
    data_batches = {topic: [] for topic in topics}

    try:
        for message in consumer:
            print("Message received")
            if message is None:
                continue

            # 메시지를 해당 토픽의 배치에 추가
            topic_name = message.topic
            data_batches[topic_name].append(message.value)

            # 배치 크기가 100 이상이면 S3에 저장하고 비웁니다.
            if len(data_batches[topic_name]) >= 100:
                print(f"Data batch for {topic_name} received")
                save_to_s3(data_batches[topic_name], topic_name.replace('_topic', '').replace('upbit_', ''))
                data_batches[topic_name] = []  # 배치를 비웁니다.
    except Exception as e:
        print(e)
    finally:
        consumer.close()


if __name__ == "__main__":
    topics = [
        "upbit_candle_topic",
        "upbit_ticker_topic",
        "upbit_orderbook_topic",
        "upbit_trade_topic",
    ]
    consume_and_save(topics)

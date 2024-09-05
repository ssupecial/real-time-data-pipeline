from kafka import KafkaConsumer
from json import loads
import psycopg2
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)

# 커서 생성
cur = conn.cursor()


def dict_to_postgres_query(table_name, data):
    columns = ", ".join(data.keys())
    values = ", ".join(["%s"] * len(data))
    values_list = []

    for key, value in data.items():
        if key == "timestamp":
            value = datetime.fromtimestamp(value / 1000.0)
        values_list.append(value)

    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    return insert_query, values_list


def save_to_postgresql(table_name, data):
    insert_query, values = dict_to_postgres_query(f"{table_name}", data)

    # 데이터 삽입
    cur.execute(insert_query, values)

    # 변경 사항 커밋
    conn.commit()


def consume_and_save(topics):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streaming",
        value_deserializer=lambda x: loads(x.decode("utf-8")),
        consumer_timeout_ms=10000000,
    )
    try:
        for message in consumer:
            print("Message received of Topic: ", message.topic)
            # print(message)
            # print(
            #     f"Topic: {message.topic},Partition: {message.partition},Offset: {message.offset},Key: {message.key},Value: {message.value}"
            # )
            data = message.value
            print("Data timestamp: ", data["timestamp"])
            save_to_postgresql(f"{message.topic.replace('_topic', '')}", data)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        cur.close()


if __name__ == "__main__":
    topics = [
        "upbit_candle_topic",
        "upbit_ticker_topic",
        "upbit_orderbook_topic",
        "upbit_trade_topic",
    ]
    consume_and_save(topics)
import requests
from kafka import KafkaProducer
from json import dumps
import time
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime


load_dotenv()

class UpbitAPI:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key

    def get_account(self):
        payload = {
            "access_key": self.access_key,
            "nonce": str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(payload, self.secret_key)
        authorization_token = f"Bearer {jwt_token}"

        headers = {
            "Authorization": authorization_token,
        }

        url = "https://api.upbit.com/v1/accounts"

        response = requests.get(url, headers=headers)
        return response.json()

    def get_candle_data(self, market, count):
        url = (
            f"https://api.upbit.com/v1/candles/minutes/1?market={market}&count={count}"
        )

        headers = {"accept": "application/json"}
        try:
            response = requests.get(url, headers=headers)
            response = response.json()[0]
            output = {
                "timestamp": response["timestamp"],
                "open": response["opening_price"],
                "high": response["high_price"],
                "low": response["low_price"],
                "trade": response["trade_price"],
                "candle_acc_trade_price": response["candle_acc_trade_price"],
                "candle_acc_trade_volume": response["candle_acc_trade_volume"],
            }
            return output
        except Exception as e:
            print("Candle Error:", e)
            return None

    def get_trade_data(self, market, count):
        url = f"https://api.upbit.com/v1/trades/ticks?market={market}&count={count}"

        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        try:
            response = response.json()[0]
            output = {
                "timestamp": response["timestamp"],
                "trade_price": response["trade_price"],
                "trade_volume": response["trade_volume"],
                "prev_closing_price": response["prev_closing_price"],
                "change_price": response["change_price"],
                "trade_time_utc": response["trade_time_utc"],
            }
            return output
        except Exception as e:
            print("Trade Error:", e)
            return None

    def get_ticker_data(self, market, count):
        url = f"https://api.upbit.com/v1/ticker?markets={market}&count={count}"

        headers = {"accept": "application/json"}
        try:
            response = requests.get(url, headers=headers)
            response = response.json()[0]

            output = {
                "timestamp": response["timestamp"],
                'trade_date': response['trade_date'],
                "trade_time": response["trade_time"],
                "opening_price": response["opening_price"],
                "high_price": response["high_price"],
                "low_price": response["low_price"],
                "trade_price": response["trade_price"],
                "prev_closing_price": response["prev_closing_price"],
                "change": response["change"],
                "change_price": response["change_price"],
                "change_rate": response["change_rate"],
                "signed_change_price": response["signed_change_price"],
                "signed_change_rate": response["signed_change_rate"],
                "trade_volume": response["trade_volume"],
                "acc_trade_price": response["acc_trade_price"],
                "acc_trade_volume": response["acc_trade_volume"],
            }
            return output
        except Exception as e:
            print("Ticker Error:", e)
            return None
            


    def get_orderbook_data(self):
        url = "https://api.upbit.com/v1/orderbook?markets=KRW-BTC&level=0"

        headers = {"accept": "application/json"}
        try:
            response = requests.get(url, headers=headers)
            response = response.json()[0]
            output = {
                "timestamp": response["timestamp"],
                "total_ask_size": response["total_ask_size"],
                "total_bid_size": response["total_bid_size"],
                "max_ask_price": response["orderbook_units"][0]["ask_price"],
                "max_bid_price": response["orderbook_units"][0]["bid_price"],
                "min_ask_price": response["orderbook_units"][-1]["ask_price"],
                "min_bid_price": response["orderbook_units"][-1]["bid_price"],
                "median_ask_price": response["orderbook_units"][len(response["orderbook_units"]) // 2]["ask_price"],
                "median_bid_price": response["orderbook_units"][len(response["orderbook_units"]) // 2]["bid_price"],
            }
        except Exception as e:
            print("Orderbook Error:", e)
            output = None
        return output


def get_candle_data_and_send_to_kafka(api_instance, market, count, interval=10):
    producer = KafkaProducer(
        acks=0,
        compression_type="gzip",
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    try:
        while True:
            data_candle = api_instance.get_candle_data(market, count)
            if data_candle is not None:
                send_to_kafka("upbit_candle_topic", data_candle, producer)
            time.sleep(interval)
    except Exception as e:
        print("Candle Error:", e)
    finally:
        producer.close()


def get_trade_data_and_send_to_kafka(api_instance, market, count, interval=10):
    producer = KafkaProducer(
        acks=0,
        compression_type="gzip",
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    try:
        while True:
            data_trade = api_instance.get_trade_data(market, count)
            if data_trade is not None:
                send_to_kafka("upbit_trade_topic", data_trade, producer)
            time.sleep(interval)
    except Exception as e:
        print("Trade Error:", e)
    finally:
        producer.close()


def get_ticker_data_and_send_to_kafka(api_instance, market, count, interval=10):
    producer = KafkaProducer(
        acks=0,
        compression_type="gzip",
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    try:
        while True:
            data_ticker = api_instance.get_ticker_data(market, count)
            if data_ticker is not None:
                send_to_kafka("upbit_ticker_topic", data_ticker, producer)
            else:
                print("Ticker data is None")
            time.sleep(interval)
    except Exception as e:
        print("Ticker Error:", e)
    finally:
        producer.close()


def get_orderbook_data_and_send_to_kafka(api_instance, interval=10):
    producer = KafkaProducer(
        acks=0,
        compression_type="gzip",
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    try:
        while True:
            data_orderbook = api_instance.get_orderbook_data()
            if data_orderbook is not None:
                send_to_kafka("upbit_orderbook_topic", data_orderbook, producer)
            time.sleep(interval)
    except Exception as e:
        print("Orderbook Error:", e)
    finally:
        producer.close()


def send_to_kafka(topic, data, producer):
    producer.send(topic, value=data)
    producer.flush()
    print(f"Data sent to Kafka topic {topic}")


def get_data_send_kafka(api_instance, market, count, interval):
    producer = KafkaProducer(
        acks=0,
        compression_type="gzip",
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    while True:
        try:
            data = api_instance.get_candle_data(market, count)
            if data is not None:
                send_to_kafka("upbit_candle_topic", data, producer)

            data = api_instance.get_trade_data(market, count)
            if data is not None:
                send_to_kafka("upbit_trade_topic", data, producer)

            data = api_instance.get_ticker_data(market, count)
            if data is not None:
                send_to_kafka("upbit_ticker_topic", data, producer)

            data = api_instance.get_orderbook_data()
            if data is not None:
                send_to_kafka("upbit_orderbook_topic", data, producer)

        except Exception as e:
            print("Sending data to Kafak Error:", e)

        time.sleep(interval)



def main():
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")

    upbit_api = UpbitAPI(access_key, secret_key)

    # get_candle_data_and_send_to_kafka(upbit_api, "KRW-BTC", 1)
    # get_trade_data_and_send_to_kafka(upbit_api, "KRW-BTC", 1)
    # get_ticker_data_and_send_to_kafka(upbit_api, "KRW-BTC", 1)
    # get_orderbook_data_and_send_to_kafka(upbit_api)

    get_data_send_kafka(upbit_api, "KRW-BTC", 1, 5)

    # while True:
    #     try:
    #         # data_candle = upbit_api.get_candle_data("KRW-BTC", 1)
    #         # data_trade = upbit_api.get_trade_data("KRW-BTC", 1)
    #         data_ticker = upbit_api.get_ticker_data("KRW-BTC", 1)
    #         # data_orderbook = upbit_api.get_orderbook_data()
    #         print(data_ticker)
    #         # send_to_kafka("upbit_candle_topic", data_candle)
    #         # send_to_kafka("upbit_trade_topic", data_trade)
    #         # print(data_ticker)
    #         # get_candle_data_and_send_to_kafka.remote(upbit_api, "KRW-BTC", 1, producer)
    #         # get_trade_data_and_send_to_kafka.remote(upbit_api, "KRW-BTC", 1, producer)
    #         # get_ticker_data_and_send_to_kafka.remote(upbit_api, "KRW-BTC", 1, producer)
    #         # get_orderbook_data_and_send_to_kafka.remote(upbit_api, producer)
 
    #     except Exception as e:
    #         print(e)
        # time.sleep(10)


if __name__ == "__main__":
    main()


# print(response.text)
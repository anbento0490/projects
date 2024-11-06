import argparse
import json
import os
import random
from datetime import datetime
from uuid import uuid4
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# --------------------------------------------------------------------------------------------- #
def order_event():
    message_type = random.choice(["A", "D", "U", "X", "F", "P", "Q", "S"])
    tracking_number = str(uuid4())
    timestamp = fake.date_time_between(start_date=datetime(2024, 10, 25), end_date="now").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    order_reference_number = ("ORD100" + str(fake.random_int(min=100000, max=999999)))
    buy_sell_indicator = random.choice(["B", "S"])
    stock = random.choice(["AAPL","AMZN","TSLA","NVDA","MSFT","META","AMD","GOOGL","INTC","NFLX"])
    shares = fake.random_int(min=100, max=10000)
    price = round(random.uniform(100, 1000), 2) 
    attribution = fake.random_int(min=100000, max=999999)

    event = {
        "message_type": message_type,
        "tracking_number": tracking_number,
        "timestamp": timestamp,
        "order_reference_number": order_reference_number,
        "buy_sell_indicator": buy_sell_indicator,
        "stock": stock,
        "shares": shares,
        "price": price,
        "attribution": attribution,
    }

    return event

# --------------------------------------------------------------------------------------------- #
def push_to_kafka(event, topic, producer):
    producer.produce(topic, json.dumps(event).encode("utf-8"), callback=event_delivery_report)
    producer.poll(0)

# --------------------------------------------------------------------------------------------- #

def gen_order_events(num_records: int, kafka_broker: str) -> None:
    producer = Producer({"bootstrap.servers": kafka_broker})

    for _ in range(num_records):
        event = order_event()
        push_to_kafka(event, "nasdaq_order_book", producer)
        producer.poll(0)

    producer.flush()
    print( f"Generated {num_records} records and sent to Kafka topic 'nasdaq_order_book'.", flush=True)

# --------------------------------------------------------------------------------------------- #
def event_delivery_report(err, msg):
    if err is not None:
        print(f"Event delivery failed: {err}", flush=True)
    else:
        print(f"Event delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}",flush=True)

# --------------------------------------------------------------------------------------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-nr", "--num_records", type=int, help="Number of records to generate", default=10000,)
    args = parser.parse_args()

    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    gen_order_events(args.num_records, kafka_broker)

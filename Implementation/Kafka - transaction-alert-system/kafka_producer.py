import json
import time
import random
from faker import Faker
from confluent_kafka import Producer

# Initialize Faker to generate fake transaction data
fake = Faker()

# Kafka configuration (local setup)
kafka_config = {
    "bootstrap.servers": "localhost:9092"   # Kafka broker address
}

# Create kafka producer instance
producer = Producer(kafka_config)

# Kafka topic name
TOPIC = "transactions"

def generate_transaction():
    """
    Generate a fake financial transactions.
    Returns:
        dict: transaction details
    """

    return {
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "merchant": fake.company(),
        "amount": round(random.uniform(10, 10000), 2),
        "currency": "USD",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "location": random.choice(["Mumbai", "London", "NewYork", "CapTown", "Sedney"])
    }

def delivery_report(err, msg):
    """
    Callback function to confirm message delivery to Kafka.

    Args:
        err: Error if message delivery falied.
        msg: Kafka message metadata if successful.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivery to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def producer_transactions():
    """
    Continuously generates and sends transactions to Kafka.
    """
    print("Starting Kafka Producer... Sending transactions to Kafka.")

    try:
        while True:
            # Generate a transaction
            transaction = generate_transaction()

            # Convert transaction to JSON string
            transaction_json = json.dumps(transaction)

            # Send to Kafka
            producer.produce(TOPIC, value=transaction_json, callback=delivery_report)

            # Ensure all messages are sent
            producer.flush()

            # Simulate transaction freqency (every 10 seconds)
            time.sleep(10)
    except KeyboardInterrupt:
        print("Producer stopped by user.")

if __name__ == "__main__":
    producer_transactions()
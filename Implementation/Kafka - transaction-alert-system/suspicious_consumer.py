import json
import redis
from confluent_kafka import Consumer, KafkaError
from database import insert_suspicious_transaction

# Connect to redis
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Kafka Configuration
kafka_config = {
    "bootstrap.servers": "localhost:9092",          # Kafka broker
    "group.id": "suspicious-transaction-group",     # Consumer group name
    "auto.offset.reset": "earliest"                 # Start reading from the beginning
}

# Create kafka consumer
consumer = Consumer(kafka_config)

# Kafka topic for suspicious transactions
SUSPICIOUS_TOPIC = "suspicious-transactions"

# Subscribe to the topic
consumer.subscribe([SUSPICIOUS_TOPIC])

def push_to_redis(transaction):
    """
    Push suspicious transactions to Redis
    """
    redis_client.publish("suspicious_alerts", json.dumps(transaction))  # Publish to Redis Pub/Sub

def consume_suspicious_transactions():
    """
    Continuously consumes suspicious transactions from Kafka
    """
    print("Listening for suspicious transactions")

    try:
        while True:
            # Poll Kafka for new message
            msg = consumer.poll(1.0)    # Timeout after 1 sec

            if msg is None:
                continue    # no new message

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Reached end of partition")
                else:
                    print(f"Consumer error: {msg.error()}")
                continue
            
            transaction = json.loads(msg.value().decode("utf-8"))

            print(f"Suspicious Transaction alert: {transaction}")
            insert_suspicious_transaction(transaction)
            push_to_redis(transaction)  # Push to Redis
    except KeyboardInterrupt:
        print("Consumer stopped by user.")

    finally:
        # Close consumer gracefully
        consumer.close()

if __name__ == "__main__":
    consume_suspicious_transactions()
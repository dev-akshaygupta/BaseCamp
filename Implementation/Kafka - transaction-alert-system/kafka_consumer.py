import json
from confluent_kafka import Consumer, KafkaError
from database import insert_all_transaction

# Kafka configuration
kafka_config = {
    "bootstrap.servers": "localhost:9092",  # Kafka broker
    "group.id": "transaction-consumer-group",   # Consumer group name
    "auto.offset.reset": "earliest" # Start reading from the beginning
}

# Create Kafka consumer instance
consumer = Consumer(kafka_config)

# Kafka topic name
TOPIC = "transactions"

# Subscribe to the topic
consumer.subscribe([TOPIC])

def consume_transactions():
    """
    Continuously consumes messages from Kafka.
    """
    print("Listening for transactions from Kafka...")

    try:
        while True:
            # Poll kafka from new messages
            msg = consumer.poll(1.0) # Timeout after 1 second

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Reached end of partiion")
                else:
                    print(f"Consumer Error: {msg.error()}")
                continue

            # Decode message from Kafka
            transaction = json.loads(msg.value().decode("utf-8"))
            
            print(f"Received transaction: {transaction}")
            insert_all_transaction(transaction)
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    
    finally:
        # Close the consumer
        consumer.close()

if __name__ == "__main__":
    consume_transactions()
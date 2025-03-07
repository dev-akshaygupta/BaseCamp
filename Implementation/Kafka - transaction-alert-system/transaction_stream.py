import faust
import datetime

# Define Faust App
app = faust.App(
    "transaction-monitor",  # application name
    broker = "kafka://localhost:9092",  # Kafka broker
    store = "memory://",    # using in-memory storage (for now)
)

# Define Kafka topic schema
class Transaction(faust.Record, serializer="json"):
    transaction_id: str
    user_id: int
    merchant: str
    amount: float
    currency: str
    timestamp: str
    location: str

# Kafka topic for transactions
transactions_topic = app.topic("transactions", value_type=Transaction)

# Define output for topic for flagged transactions
suspicious_transactions_topic = app.topic("suspicious-transactions", value_type=Transaction)

# Store user transaction history in-memory
user_transactions = {}

@app.agent(transactions_topic)
async def detect_suspicious_transactions(transactions):
    """
    Read transactions from Kafka and flags suspicious ones.
    Suspicious criteria: 
        - Amount > $5000
        - Transaction from different countries within 10 minutes
        - More than 3 transactions within 1 minute
    """
    async for transaction in transactions:

        user_id = transaction.user_id
        current_time = datetime.datetime.strptime(transaction.timestamp, "%Y-%m-%d %H:%M:%S")

        if user_id not in user_transactions:
            user_transactions[user_id] = []

        user_transactions[user_id].append({
            "location": transaction.location,
            "timestamp": current_time
        })

        # Keep only the last 10 minutes of transactions
        user_transactions[user_id] = [
            t for t in user_transactions[user_id]
            if (current_time - t["timestamp"]).total_seconds() <= 600   # 10 minutes
        ]

        # Amount-based fraud detection
        if transaction.amount > 5000:
            print(f"High amount transaction detected: {transaction}")
            await suspicious_transactions_topic.send(value=transaction) # sent to flagged topic

        # Geo-based fraud detection
        unique_location = {t["location"] for t in user_transactions[user_id]}
        if len(unique_location) > 1:
            print(f"Geo-based anomaly detected: {transaction}")
            await suspicious_transactions_topic.send(value=transaction) # sent to flagged topic
        
        # High-Frequency fraud detection
        recent_transaction = [
            t for t in user_transactions[user_id]
            if (current_time - t["timestamp"]).total_seconds() <= 60    # last 1 minute
        ]
        if len(recent_transaction) > 1:
            print(f"High-frequency spending detected: {transaction}")
            await suspicious_transactions_topic.send(value=transaction) # sent to flagged topic


if __name__ == "__main__":
    app.main()
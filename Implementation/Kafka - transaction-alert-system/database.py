import psycopg2

DB_PARAMS = {
    "dbname": "finance",
    "user": "akshay",
    "password": "gupta",
    "host": "localhost",
    "port": "5432"
}

def create_table():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    cursor.execute("""
        DROP TABLE IF EXISTS suspicious_transactions;
        CREATE TABLE IF NOT EXISTS suspicious_transactions (
            transaction_id TEXT PRIMARY KEY,
            user_id INTEGER,
            merchant TEXT,
            amount REAL,
            currency TEXT,
            timestamp TEXT,       
            location TEXT
        );

        DROP TABLE IF EXISTS all_transactions;         
        CREATE TABLE IF NOT EXISTS all_transactions (
            transaction_id TEXT PRIMARY KEY,
            user_id INTEGER,
            merchant TEXT,
            amount REAL,
            currency TEXT,
            timestamp TEXT,       
            location TEXT
        )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

def insert_suspicious_transaction(transaction):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO suspicious_transactions (transaction_id, user_id, merchant, amount, currency, timestamp, location) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction["transaction_id"],
            transaction["user_id"],
            transaction["merchant"],
            transaction["amount"],
            transaction["currency"],
            transaction["timestamp"],
            transaction["location"]
        )
    )

    conn.commit()
    cursor.close()
    conn.close()

def insert_all_transaction(transaction):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO all_transactions (transaction_id, user_id, merchant, amount, currency, timestamp, location) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction["transaction_id"],
            transaction["user_id"],
            transaction["merchant"],
            transaction["amount"],
            transaction["currency"],
            transaction["timestamp"],
            transaction["location"]
        )
    )

    conn.commit()
    cursor.close()
    conn.close()

# Create table when executed
create_table()
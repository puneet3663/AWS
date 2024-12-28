## in this example , a kafka is producing data with 3 columns. here this py file will read data from kafka 3rd party producer and 
## store the results in RDS with the help of consumer running on MSKy

from kafka import KafkaConsumer
import psycopg2
import json

# Kafka configurations
KAFKA_TOPIC = "your_kafka_topic"
KAFKA_BROKERS = ["your-msk-broker1:9092", "your-msk-broker2:9092"]

# PostgreSQL configurations
DB_HOST = "your-rds-host.amazonaws.com"
DB_NAME = "your_database_name"
DB_USER = "your_database_user"
DB_PASSWORD = "your_database_password"
DB_TABLE = "your_table_name"

def connect_to_db():
    """Establishes a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def insert_into_db(connection, name, phone, transaction_amount):
    """Inserts a record into the database."""
    try:
        with connection.cursor() as cursor:
            query = f"""
                INSERT INTO {DB_TABLE} (name, phone, transaction_amount) 
                VALUES (%s, %s, %s)
            """
            cursor.execute(query, (name, phone, transaction_amount))
            connection.commit()
    except Exception as e:
        print(f"Error inserting into the database: {e}")
        connection.rollback()

def consume_from_kafka():
    """Consumes messages from Kafka and writes them to the database."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    connection = connect_to_db()

    print("Starting Kafka consumer...")
    for message in consumer:
        try:
            data = message.value
            name = data.get("name")
            phone = data.get("phone")
            transaction_amount = data.get("transaction_amount")

            if name and phone and transaction_amount is not None:
                insert_into_db(connection, name, phone, transaction_amount)
                print(f"Inserted: {name}, {phone}, {transaction_amount}")
            else:
                print(f"Invalid data: {data}")
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    consume_from_kafka()

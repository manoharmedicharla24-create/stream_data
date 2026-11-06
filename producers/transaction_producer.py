import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def generate_normal_transaction():

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"USER_{random.randint(1000, 9999)}",
        "amount": round(random.uniform(100, 10000), 2),
        "merchant": random.choice([
            "Amazon",
            "Flipkart",
            "Walmart",
            "Uber",
            "Swiggy",
            "Netflix"
        ]),
        "location": random.choice([
            "Hyderabad",
            "Bangalore",
            "Chennai"
        ]),
        "device_id": f"DEVICE_{random.randint(100, 999)}",
        "timestamp": datetime.now().isoformat(),
        "transaction_type": "NORMAL"
    }

    return transaction


def generate_fraud_transaction():

    fraud_type = random.choice([
        "HIGH_AMOUNT",
        "SUSPICIOUS_LOCATION",
        "SUSPICIOUS_MERCHANT"
    ])

    if fraud_type == "HIGH_AMOUNT":

        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": f"USER_{random.randint(1000, 9999)}",
            "amount": round(random.uniform(300000, 500000), 2),
            "merchant": random.choice([
                "Amazon",
                "Flipkart",
                "Walmart"
            ]),
            "location": random.choice([
                "Hyderabad",
                "Bangalore",
                "Chennai"
            ]),
            "device_id": f"DEVICE_{random.randint(100, 999)}",
            "timestamp": datetime.now().isoformat(),
            "transaction_type": "HIGH_AMOUNT_FRAUD"
        }

    elif fraud_type == "SUSPICIOUS_LOCATION":

        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": f"USER_{random.randint(1000, 9999)}",
            "amount": round(random.uniform(50000, 150000), 2),
            "merchant": "Amazon",
            "location": random.choice([
                "Delhi",
                "Mumbai"
            ]),
            "device_id": f"DEVICE_{random.randint(100, 999)}",
            "timestamp": datetime.now().isoformat(),
            "transaction_type": "LOCATION_FRAUD"
        }

    else:

        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": f"USER_{random.randint(1000, 9999)}",
            "amount": round(random.uniform(100000, 250000), 2),
            "merchant": random.choice([
                "Netflix",
                "Uber"
            ]),
            "location": random.choice([
                "Delhi",
                "Mumbai"
            ]),
            "device_id": f"DEVICE_{random.randint(100, 999)}",
            "timestamp": datetime.now().isoformat(),
            "transaction_type": "MERCHANT_FRAUD"
        }

    return transaction


def generate_velocity_transactions():

    transactions = []

    user_id = "USER_VELOCITY_001"
    device_id = "DEVICE_VELOCITY_001"

    amounts = [
        500,
        700,
        900,
        1200
    ]

    for amount in amounts:

        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": amount,
            "merchant": "Amazon",
            "location": "Hyderabad",
            "device_id": device_id,
            "timestamp": datetime.now().isoformat(),
            "transaction_type": "VELOCITY_TEST"
        }

        transactions.append(transaction)

    return transactions


def send_transaction(transaction):

    producer.send(
        "transactions",
        value=transaction
    )

    producer.flush()

    print(
        f"Sent transaction | "
        f"User: {transaction['user_id']} | "
        f"Type: {transaction['transaction_type']} | "
        f"Amount: {transaction['amount']}"
    )


print("🚀 Transaction Producer Started")

velocity_test_done = False

while True:

    # Run velocity test once
    if not velocity_test_done:

        print("\n🔥 Sending VELOCITY TEST transactions...\n")

        velocity_transactions = generate_velocity_transactions()

        for transaction in velocity_transactions:

            send_transaction(transaction)

            time.sleep(2)

        velocity_test_done = True

        print(
            "\n✅ Velocity test completed\n"
        )

    # Continue normal traffic
    transaction = generate_normal_transaction()

    # Occasionally generate normal fraud
    if random.random() < 0.10:
        transaction = generate_fraud_transaction()

    send_transaction(transaction)

    time.sleep(2)
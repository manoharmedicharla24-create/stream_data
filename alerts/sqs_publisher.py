
import json
import time

import boto3
import psycopg2
from botocore.exceptions import ClientError


# =========================================================
# AWS CONFIGURATION
# =========================================================

AWS_REGION = "ap-south-1"

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)

# The existing queue triggers AWS Lambda.
LAMBDA_QUEUE_NAME = "fraud-alert-queue"

# The new queue is consumed by the local Fraud Agent.
AGENT_QUEUE_NAME = "fraud-agent-queue"


def get_queue_url(queue_name):
    response = sqs.get_queue_url(
        QueueName=queue_name
    )
    return response["QueueUrl"]


try:
    LAMBDA_QUEUE_URL = get_queue_url(
        LAMBDA_QUEUE_NAME
    )

    AGENT_QUEUE_URL = get_queue_url(
        AGENT_QUEUE_NAME
    )

    print(f"✅ Lambda queue: {LAMBDA_QUEUE_URL}")
    print(f"✅ Agent queue:  {AGENT_QUEUE_URL}")

except ClientError as error:
    print(f"❌ Could not find an SQS queue: {error}")
    raise SystemExit(1)


# =========================================================
# POSTGRESQL CONFIGURATION
# =========================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "database": "fraud_detection",
    "user": "fraud_user",
    "password": "fraud_password"
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_new_alerts(last_alert_id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    alert_id,
                    transaction_id,
                    user_id,
                    amount,
                    fraud_score,
                    risk_level,
                    reason,
                    created_at
                FROM fraud_alerts
                WHERE alert_id > %s
                ORDER BY alert_id
                """,
                (last_alert_id,)
            )

            return cursor.fetchall()

    finally:
        connection.close()


# =========================================================
# SEND ONE ALERT TO BOTH QUEUES
# =========================================================

def send_to_sqs(alert):
    (
        alert_id,
        transaction_id,
        user_id,
        amount,
        fraud_score,
        risk_level,
        reason,
        created_at
    ) = alert

    message = {
        "alert_id": alert_id,
        "transaction_id": transaction_id,
        "user_id": user_id,
        "amount": float(amount) if amount is not None else None,
        "fraud_score": fraud_score,
        "risk_level": risk_level,
        "reason": reason,
        "created_at": str(created_at)
    }

    body = json.dumps(message)

    targets = [
        ("Lambda", LAMBDA_QUEUE_URL),
        ("Fraud Agent", AGENT_QUEUE_URL)
    ]

    all_sent = True

    for target_name, queue_url in targets:
        try:
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=body
            )

            print(
                f"✅ Alert {alert_id} sent to {target_name} "
                f"| Message ID: {response['MessageId']}"
            )

        except ClientError as error:
            all_sent = False

            print(
                f"❌ Failed to send alert {alert_id} "
                f"to {target_name}: {error}"
            )

    return all_sent


# =========================================================
# CONTINUOUS PUBLISHER
# =========================================================

last_alert_id = 0

print("\n🚀 SQS Fraud Alert Publisher Started")
print("📡 Publishing PostgreSQL alerts to both queues...\n")

while True:
    try:
        alerts = get_new_alerts(last_alert_id)

        for alert in alerts:
            success = send_to_sqs(alert)

            if not success:
                # Retry this alert during the next polling cycle.
                # A queue that already received it may receive a duplicate.
                break

            last_alert_id = alert[0]

        time.sleep(3)

    except psycopg2.Error as error:
        print(f"❌ PostgreSQL error: {error}")
        time.sleep(5)

    except Exception as error:
        print(f"❌ Publisher error: {error}")
        time.sleep(5)

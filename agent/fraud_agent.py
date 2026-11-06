import json
import os
import time

import boto3
import psycopg2
import requests
from psycopg2.extras import RealDictCursor

# ---------------- CONFIGURATION ----------------

AWS_REGION = "ap-south-1"
QUEUE_NAME = "fraud-agent-queue"

OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL_NAME = "llama3.2:3b"

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "fraud_detection",
    "user": "fraud_user",
    "password": "fraud_password",
}

sqs = boto3.client("sqs", region_name=AWS_REGION)

# ---------------- DATABASE ----------------

def get_transaction_history(user_id):
    """Retrieve real transaction and fraud-alert records for a user."""

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT transaction_id, user_id, amount, merchant,
                       location, device_id, transaction_timestamp,
                       transaction_type, fraud_score, risk_level
                FROM transactions
                WHERE user_id = %s
                ORDER BY transaction_timestamp DESC
                LIMIT 10
                """,
                (user_id,),
            )
            transactions = [dict(row) for row in cursor.fetchall()]

            cursor.execute(
                """
                SELECT alert_id, transaction_id, amount, fraud_score,
                       risk_level, reason, created_at
                FROM fraud_alerts
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (user_id,),
            )
            alerts = [dict(row) for row in cursor.fetchall()]

    return {
        "recent_transactions": transactions,
        "recent_fraud_alerts": alerts,
    }


# ---------------- AI INVESTIGATION ----------------

def investigate_with_ai(alert, history):
    """Ask the local LLM to investigate using supplied evidence."""

    evidence = json.dumps(
        {"current_alert": alert, "database_evidence": history},
        indent=2,
        default=str,
    )

    system_prompt = """
You are an AI fraud investigation analyst.

Rules:
1. Use only facts contained in the supplied evidence.
2. Never invent names, transactions, amounts, locations, or history.
3. Distinguish observed facts from possible explanations.
4. If evidence is missing, explicitly say so.
5. A high risk score is a signal, not proof of fraud.
6. Do not claim to have blocked an account or taken an action.
7. Recommend manual review when evidence is insufficient.

Return a JSON object with these fields:
risk_assessment, findings, missing_evidence, recommendation,
explanation.

The findings and missing_evidence fields must be arrays of strings.
The recommendation must be a suggested action, not a claim that an
action has already happened.
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL_NAME,
            "stream": False,
            "format": "json",
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": "Investigate this alert using the evidence:\n"
                    + evidence,
                },
            ],
        },
        timeout=180,
    )
    response.raise_for_status()

    result = response.json()["message"]["content"]

    # Validate the model's response as JSON before presenting it.
    report = json.loads(result)
    return report


# ---------------- SQS AGENT ----------------

def main():
    queue_response = sqs.get_queue_url(QueueName=QUEUE_NAME)
    queue_url = queue_response["QueueUrl"]

    print("AI Fraud Investigation Agent started.")
    print(f"Queue: {QUEUE_NAME}")
    print(f"LLM: {MODEL_NAME}")
    print("Waiting for fraud alerts...\n")

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=240,
            )

            for message in response.get("Messages", []):
                try:
                    alert = json.loads(message["Body"])
                    user_id = alert.get("user_id")

                    if not user_id:
                        print("Alert has no user_id; skipping investigation.")
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message["ReceiptHandle"],
                        )
                        continue

                    print("\n" + "=" * 60)
                    print(f"Investigating user: {user_id}")
                    print("Retrieving transaction history from PostgreSQL...")

                    history = get_transaction_history(user_id)

                    print(
                        f"Transactions retrieved: "
                        f"{len(history['recent_transactions'])}"
                    )
                    print("Sending evidence to local AI model...")

                    report = investigate_with_ai(alert, history)

                    print("\nAI INVESTIGATION REPORT")
                    print(json.dumps(report, indent=2, ensure_ascii=False))

                    # Delete only after successful investigation.
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message["ReceiptHandle"],
                    )
                    print("\nAlert processed successfully.")

                except (
                    requests.RequestException,
                    psycopg2.Error,
                    json.JSONDecodeError,
                    KeyError,
                    ValueError,
                ) as error:
                    print(f"Investigation failed: {error}")
                    print("Message will remain available for retry.")

        except Exception as error:
            print(f"Agent error: {error}")
            time.sleep(5)


if __name__ == "__main__":
    main()
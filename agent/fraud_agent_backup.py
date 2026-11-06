import json
import time
import boto3
from botocore.exceptions import ClientError

# AWS SQS configuration
AWS_REGION = "ap-south-1"
QUEUE_NAME = "fraud-alert-queue"

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)


def connect_to_queue():
    """Find the existing SQS queue."""
    try:
        response = sqs.get_queue_url(
            QueueName=QUEUE_NAME
        )
        queue_url = response["QueueUrl"]
        print(f"✅ SQS queue found: {queue_url}")
        return queue_url

    except ClientError as error:
        print("❌ Could not access the SQS queue.")
        print(f"AWS error: {error}")
        raise SystemExit(1)


def investigate_fraud(alert):
    """Generate a rule-based investigation report."""
    amount = alert.get("amount")
    score = alert.get("fraud_score")
    risk = str(alert.get("risk_level") or "UNKNOWN").upper()
    reason = str(alert.get("reason") or "")

    findings = []

    if amount is not None:
        amount = float(amount)

        if amount >= 300000:
            findings.append(
                f"Very high transaction amount: ₹{amount:,.2f}"
            )
        elif amount >= 100000:
            findings.append(
                f"High transaction amount: ₹{amount:,.2f}"
            )

    if "VELOCITY" in reason.upper():
        findings.append(
            "Multiple transactions detected within a short time window."
        )

    if score is not None and float(score) >= 70:
        findings.append(f"High rule-based fraud score: {score}")

    if reason:
        findings.append(f"Detection rule triggered: {reason}")

    if not findings:
        findings.append("Insufficient alert details for investigation.")

    if risk == "HIGH":
        recommendation = (
            "Urgent review: verify the transaction before taking action."
        )
    elif risk == "MEDIUM":
        recommendation = "Require additional verification."
    elif risk == "LOW":
        recommendation = "No additional action suggested by this alert."
    else:
        recommendation = "Manual review required."

    return {
        "alert_id": alert.get("alert_id"),
        "user_id": alert.get("user_id"),
        "transaction_id": alert.get("transaction_id"),
        "amount": amount,
        "fraud_score": score,
        "risk_level": risk,
        "reason": reason,
        "findings": findings,
        "recommendation": recommendation
    }


def print_report(report):
    print("\n" + "=" * 60)
    print("🤖 FRAUD INVESTIGATION AGENT")
    print("=" * 60)
    print(f"Alert ID       : {report['alert_id']}")
    print(f"User ID        : {report['user_id']}")
    print(f"Transaction ID : {report['transaction_id']}")
    print(f"Amount         : {report['amount']}")
    print(f"Fraud Score    : {report['fraud_score']}")
    print(f"Risk Level     : {report['risk_level']}")
    print(f"Reason         : {report['reason']}")

    print("\nINVESTIGATION FINDINGS:")
    for finding in report["findings"]:
        print(f" - {finding}")

    print("\nRECOMMENDATION:")
    print(report["recommendation"])
    print("=" * 60)


def main():
    queue_url = connect_to_queue()

    print("🤖 Fraud Investigation Agent Started")
    print("📡 Waiting for fraud alerts from SQS...\n")

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=60
            )

            for message in response.get("Messages", []):
                try:
                    alert = json.loads(message["Body"])
                    report = investigate_fraud(alert)
                    print_report(report)

                    # Delete only after successful investigation.
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message["ReceiptHandle"]
                    )

                    print("✅ Alert processed successfully.\n")

                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
                    print(f"❌ Invalid alert; message retained: {error}")

                except ClientError as error:
                    print(f"❌ AWS message-processing error: {error}")

        except ClientError as error:
            print(f"❌ SQS polling error: {error}")
            time.sleep(5)


if __name__ == "__main__":
    main()
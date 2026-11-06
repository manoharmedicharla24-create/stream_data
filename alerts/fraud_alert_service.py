import time
import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "database": "fraud_detection",
    "user": "fraud_user",
    "password": "fraud_password"
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def check_new_alerts(last_alert_id):
    connection = get_connection()
    cursor = connection.cursor()

    query = """
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
        ORDER BY alert_id;
    """

    cursor.execute(query, (last_alert_id,))
    alerts = cursor.fetchall()

    cursor.close()
    connection.close()

    return alerts


last_alert_id = 0

print("🚨 Fraud Alert Service Started")
print("Waiting for HIGH-risk fraud alerts...\n")

while True:

    alerts = check_new_alerts(last_alert_id)

    for alert in alerts:

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

        print("\n" + "=" * 60)
        print("🚨 FRAUD ALERT")
        print("=" * 60)

        print(f"Alert ID       : {alert_id}")
        print(f"Transaction ID : {transaction_id}")
        print(f"User ID        : {user_id}")
        print(f"Amount         : ₹{amount}")
        print(f"Fraud Score    : {fraud_score}")
        print(f"Risk Level     : {risk_level}")
        print(f"Reason         : {reason}")
        print(f"Created At     : {created_at}")

        print("=" * 60)

        last_alert_id = alert_id

    time.sleep(3)
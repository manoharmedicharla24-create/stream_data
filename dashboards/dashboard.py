
import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(
    page_title="AI Fraud Detection",
    page_icon="🛡️",
    layout="wide",
)

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "fraud_detection",
    "user": "fraud_user",
    "password": "fraud_password",
}


@st.cache_data(ttl=5)
def load_data(query):
    with psycopg2.connect(**DB_CONFIG) as conn:
        return pd.read_sql_query(query, conn)


st.title("🛡️ Real-Time AI Fraud Detection")
st.caption("Transaction monitoring and fraud investigation dashboard")

try:
    transactions = load_data("""
        SELECT transaction_id, user_id, amount, merchant, location,
               fraud_score, risk_level, transaction_timestamp
        FROM transactions
        ORDER BY transaction_timestamp DESC
        LIMIT 1000
    """)

    alerts = load_data("""
        SELECT alert_id, transaction_id, user_id, amount, fraud_score,
               risk_level, reason, created_at
        FROM fraud_alerts
        ORDER BY created_at DESC
        LIMIT 500
    """)

except Exception as error:
    st.error(f"Could not connect to PostgreSQL: {error}")
    st.info(
        "Check that the fraud-postgres Docker container is running "
        "and PostgreSQL is available on port 5434."
    )
    st.stop()


total_transactions = len(transactions)
total_alerts = len(alerts)
high_risk_alerts = (
    int((alerts["risk_level"] == "HIGH").sum())
    if not alerts.empty else 0
)
high_risk_transactions = (
    int((transactions["risk_level"] == "HIGH").sum())
    if not transactions.empty else 0
)

col1, col2, col3, col4 = st.columns(4)

col1.metric("Transactions Loaded", f"{total_transactions:,}")
col2.metric("Fraud Alerts Loaded", f"{total_alerts:,}")
col3.metric("High-Risk Alerts", f"{high_risk_alerts:,}")
col4.metric("High-Risk Transactions", f"{high_risk_transactions:,}")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Transaction Risk Distribution")

    if not transactions.empty:
        risk_counts = (
            transactions["risk_level"]
            .fillna("UNKNOWN")
            .value_counts()
        )
        st.bar_chart(risk_counts)
    else:
        st.info("No transactions found yet.")

with right:
    st.subheader("Fraud Alerts by Risk Level")

    if not alerts.empty:
        alert_counts = (
            alerts["risk_level"]
            .fillna("UNKNOWN")
            .value_counts()
        )
        st.bar_chart(alert_counts)
    else:
        st.info("No fraud alerts found yet.")

st.divider()

st.subheader("Recent Transactions")
if not transactions.empty:
    st.dataframe(transactions, use_container_width=True, hide_index=True)
else:
    st.info("No transactions available.")

st.subheader("Fraud Investigation Alerts")
if not alerts.empty:
    st.dataframe(alerts, use_container_width=True, hide_index=True)
else:
    st.info("No fraud alerts available.")

st.caption(
    "Dashboard refresh: use the refresh button or rerun the app. "
    "Displayed metrics reflect the records loaded by the queries."
)

if st.button("Refresh Dashboard"):
    st.cache_data.clear()
    st.rerun()

import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime

def fetch_and_store_reliance_monthly_data():
    # Step 1: Connect to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        port=3306,
        database="market_tracker"
    )
    cursor = conn.cursor()

    # Step 2: Create table if it doesn’t exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reliance (
            date DATE PRIMARY KEY,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE
        )
    """)

    # Step 3: Get the latest date in the table
    cursor.execute("SELECT MAX(date) FROM reliance")
    result = cursor.fetchone()
    latest_date = result[0]

    # Step 4: Fetch data from Yahoo Finance
    df = yf.Ticker("RELIANCE.NS").history(period="10y", interval="1mo")
    df = df[['Open', 'High', 'Low', 'Close']].reset_index()
    df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close'}, inplace=True)

    # Step 5: Filter new rows
    if latest_date:
        df = df[df['date'] > pd.to_datetime(latest_date)]

    # Step 6: Insert new data
    if df.empty:
        print("✅ No new data to insert for Reliance.")
    else:
        insert_query = "INSERT INTO reliance(date, open, high, low, close) VALUES (%s, %s, %s, %s, %s)"
        data_to_insert = df[['date', 'open', 'high', 'low', 'close']].values.tolist()

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ Inserted {cursor.rowcount} new rows into reliance.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_store_reliance_monthly_data()

import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime

def fetch_and_store_new_monthly_data():
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
        CREATE TABLE IF NOT EXISTS nifty_50_monthly (
            date DATE PRIMARY KEY,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE
        )
    """)

    # Step 3: Get the latest date in the table
    cursor.execute("SELECT MAX(date) FROM nifty_50_monthly")
    result = cursor.fetchone()
    latest_date = result[0]

    # Step 4: Fetch data from Yahoo Finance
    df = yf.Ticker("^NSEI").history(period="10y", interval="1mo")
    df = df[['Open', 'High', 'Low', 'Close']].reset_index()
    df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close'}, inplace=True)

    # Step 5: Filter new rows
    if latest_date:
        df = df[df['date'] > pd.to_datetime(latest_date)]

    # Step 6: Insert new data
    if df.empty:
        print("✅ No new data to insert.")
    else:
        insert_query = "INSERT INTO nifty_50_monthly (date, open, high, low, close) VALUES (%s, %s, %s, %s, %s)"
        data_to_insert = df[['date', 'open', 'high', 'low', 'close']].values.tolist()

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ Inserted {cursor.rowcount} new rows into nifty_50_monthly.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_store_new_monthly_data()

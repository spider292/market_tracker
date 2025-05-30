from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)  # Allow cross-origin requests from React frontend

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        port=3306,
        database="market_tracker"
    )

@app.route("/nifty-data", methods=["GET"])
def get_nifty_data():
    lower_boundary = float(request.args.get("lower"))
    upper_boundary = float(request.args.get("upper"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT open, high, low, close
        FROM nifty_50_monthly
        ORDER BY date ASC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    filtered_data = []
    for row in rows:
        filtered_row = {}
        if lower_boundary <= row["open"] <= upper_boundary:
            filtered_row["open"] = row["open"]
        if lower_boundary <= row["high"] <= upper_boundary:
            filtered_row["high"] = row["high"]
        if lower_boundary <= row["low"] <= upper_boundary:
            filtered_row["low"] = row["low"]
        if lower_boundary <= row["close"] <= upper_boundary:
            filtered_row["close"] = row["close"]
        if filtered_row:
            filtered_data.append(filtered_row)

    return jsonify(filtered_data)

@app.route("/reliance-data", methods=["GET"])
def get_reliance_data():
    lower_boundary = float(request.args.get("lower"))
    upper_boundary = float(request.args.get("upper"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT open, high, low, close
        FROM reliance
        ORDER BY date ASC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    filtered_data = []
    for row in rows:
        filtered_row = {}
        if lower_boundary <= row["open"] <= upper_boundary:
            filtered_row["open"] = row["open"]
        if lower_boundary <= row["high"] <= upper_boundary:
            filtered_row["high"] = row["high"]
        if lower_boundary <= row["low"] <= upper_boundary:
            filtered_row["low"] = row["low"]
        if lower_boundary <= row["close"] <= upper_boundary:
            filtered_row["close"] = row["close"]
        if filtered_row:
            filtered_data.append(filtered_row)

    return jsonify(filtered_data)

if __name__ == "__main__":
    app.run(debug=True)

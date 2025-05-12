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
        SELECT date, open, high, low, close
        FROM nifty_50_monthly
        WHERE open BETWEEN %s AND %s
           OR high BETWEEN %s AND %s
           OR low BETWEEN %s AND %s
           OR close BETWEEN %s AND %s
        ORDER BY date ASC
    """
    cursor.execute(query, (lower_boundary, upper_boundary, lower_boundary, upper_boundary,
                           lower_boundary, upper_boundary, lower_boundary, upper_boundary))

    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(results)

if __name__ == "__main__":
    app.run(debug=True)

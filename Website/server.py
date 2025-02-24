from flask import Flask, render_template, request
import psycopg2
import os
import datetime
from dotenv import load_dotenv

app = Flask(__name__)

# Load environment variables from a .env file
load_dotenv()
CONN_STR = os.getenv('DATABASE_URL')


# Get all data from the table
def get_data():
    # Connect to your database using the connection string.
    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM DailyVideoData;")
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return headers, rows

# GET
@app.route('/')
def index():
    headers, rows = get_data()
    return render_template('index.html', headers=headers, rows=rows)

@app.route('/search', methods=['GET'])
def search():
    category = request.args.get('category')
    value = request.args.get('value')
    headers, rows = None, None
    if category and value:
        headers, rows = search_data(category, value)
    return render_template('search.html', headers=headers, rows=rows, category=category, value=value)

@app.route('/other')
def other():
    headers, rows = get_data()
    return render_template('other.html', headers=headers, rows=rows)



# Search for rows that match a specific value
def search_data(category, value):
    # Allowed columns to search on.
    allowed_columns = ['post_url', 'creator_username', 'marketing_associate', 'app', 'create_time', 'log_time']
    if category not in allowed_columns:
        return None, None

    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()

    # For date columns, convert user input and adjust the query.
    if category in ['create_time', 'log_time']:
        try:
            # Convert user input from "m/d/Y" format to a datetime object.
            dt = datetime.datetime.strptime(value, "%m/%d/%Y")
        except ValueError:
            # If the conversion fails, return no results.
            return None, None

        # Format the date as YYYY-MM-DD.
        formatted_value = dt.strftime("%Y-%m-%d")
        # Use the DATE() function to extract the date part from the timestamp.
        query = f"SELECT * FROM DailyVideoData WHERE DATE({category}) = %s;"
        cursor.execute(query, (formatted_value,))
    else:
        # For non-date columns, use a simple equality check.
        query = f"SELECT * FROM DailyVideoData WHERE {category} = %s;"
        cursor.execute(query, (value,))

    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return headers, rows



if __name__ == '__main__':
    app.run(debug=True)
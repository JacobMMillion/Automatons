from flask import Flask, render_template, request, Response
import psycopg2
import os
import datetime
from dotenv import load_dotenv

app = Flask(__name__)

# Load environment variables from a .env file
load_dotenv()
CONN_STR = os.getenv('DATABASE_URL')
USER = os.getenv('USERNAME')
PW = os.getenv('PASSWORD')

# Authentication
def check_auth(username, password):
    return username == USER and password == PW

def authenticate():
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    decorated.__name__ = f.__name__
    return decorated


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
@requires_auth
def index():
    headers, rows = get_data()
    return render_template('index.html', headers=headers, rows=rows)

@app.route('/search', methods=['GET'])
@requires_auth
def search():
    category = request.args.get('category')
    value = request.args.get('value')
    headers, rows = None, None
    if category and value:
        headers, rows = search_data(category, value)
    return render_template('search.html', headers=headers, rows=rows, category=category, value=value)

@app.route('/other')
@requires_auth
def other():
    headers, rows = get_data()
    return render_template('other.html', headers=headers, rows=rows)

@app.route('/graph', methods=['GET', 'POST'])
@requires_auth
def graph():
    if request.method == 'POST':
        url = request.form.get('url')
        headers, rows = search_data('post_url', url)
        
        # Determine column indices.
        try:
            date_index = headers.index("log_time")
            views_index = headers.index("view_count")
            likes_index = headers.index("num_likes")
            comments_index = headers.index("comment_count")
        except ValueError:
            return render_template('graph.html', error="Required columns not found", data=None, url=url)
        
        time_series = []
        for row in rows:
            date_val = row[date_index]
            if isinstance(date_val, datetime.datetime):
                dt = date_val
            else:
                try:
                    dt = datetime.datetime.strptime(date_val, "%Y-%m-%d")
                except ValueError:
                    continue  # Skip rows with bad date formats.
            try:
                views = float(row[views_index])
            except (ValueError, TypeError):
                views = 0
            try:
                likes = float(row[likes_index])
            except (ValueError, TypeError):
                likes = 0
            try:
                comments = float(row[comments_index])
            except (ValueError, TypeError):
                comments = 0
            
            time_series.append({
                'date': dt.strftime("%m/%d/%Y"),
                'views': views,
                'likes': likes,
                'comments': comments
            })
        
        time_series.sort(key=lambda x: datetime.datetime.strptime(x['date'], "%m/%d/%Y"))
        
        return render_template('graph.html', data=time_series, url=url)
    else:
        return render_template('graph.html')



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

# ### 1. Import required Libraries

# %%
import requests
import sqlite3
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# %% [markdown]
# ### 2. All constants used in notebook

# %%
base_url = "https://de-demo-api.adtriba.app/"
endpoint = "v1/api/data" 
api_key = "woope1Pei5zieg"
from_date = "2021-08-01" 
to_date = "2021-09-30"    
db_file_name ="fashion_brand_logs.db"

# %% [markdown]
# 
# ### 3. Fetch Tracking Logs: Defines a function to fetch tracking logs from an API using the provided API details and query parameters. It handles the API response and prints the fetched logs.
# 

# %%
# API details
headers = {
    "x-api-key": api_key
}

def fetch_tracking_logs(from_date=None, to_date=None, offset=0, limit=10000):
    params = {
        "offset": offset,
        "limit": limit
    }

    if from_date:
        params["from_date"] = from_date
    if to_date:
        params["to_date"] = to_date

    response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)

    if response.status_code == 200:
        return response.json()  
    else:
        #print(f"Error {response.status_code}: {response.text}")
        return None

# %% [markdown]
# 
# ### 4. Parse Query Parameters from Logs : Defines a function to parse query parameters from the fetched logs and prints the parsed logs.

# %%
def parse_logs(logs):
    parsed_logs = []
    for log in logs:
        parsed_logs.append(
            (
                log.get("log_id"),
                log.get("cookie_id"),
                log.get("location"),
                log.get("referrer_domain"),
                str(log.get("log_ts")),
                log.get("action"),
            )
        )
    return parsed_logs

# %% [markdown]
# 
# ### 5. Store Logs in SQLite Database : Stores the parsed logs in an SQLite database by creating a table and inserting the data into it.

# %%
def store_logs_in_db(parsed_logs, db_file_name):
    try:
        conn = sqlite3.connect(db_file_name)
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS pageviews (
            log_id TEXT,
            cookie_id TEXT,
            location TEXT,
            referrer_domain TEXT,
            log_ts TEXT,
            action TEXT
        )
        ''')
        cursor.execute('DELETE FROM pageviews')
        
        cursor.executemany('''
        INSERT INTO pageviews (log_id, cookie_id, location, referrer_domain, log_ts, action)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', parsed_logs)

        conn.commit()
    finally:
        conn.close()

# %% [markdown]
# 
# ### 6. Extract Query Parameters and Apply Channel Mapping Rules : Defines a function to extract query parameters from the logs and apply channel mapping rules to categorize the logs into different channels such as "Organic Search", "Social", "Email", "Paid Search", "Referral", and "Direct".

# %%
def extract_query_params(logs):
    extracted_data = []
    for log in logs:
        url = log['location']
        referrer_domain = log['referrer_domain']
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        utm_source = query_params.get('utm_source', [None])[0]
        utm_campaign = query_params.get('utm_campaign', [None])[0]
        utm_medium = query_params.get('utm_medium', [None])[0]
        gclid = query_params.get('gclid', [None])[0]
        
        channel = "Unknown"
        
        # Channel Mapping Rules
        if utm_medium == 'organic':
            channel = "Organic Search"
        elif utm_medium and any(prefix in utm_medium.lower() for prefix in ['social', 'sm']):
            channel = "Social"
        elif utm_source and utm_source.lower() in ['facebook', 'instagram', 'ig', 'fb', 'linkedin']:
            channel = "Social"
        elif utm_medium == 'email':
            channel = "Email"
        elif utm_medium and any(keyword in utm_medium.lower() for keyword in ['cpc', 'ppc', 'paid', 'paidsearch']):
            channel = "Paid Search"
        elif gclid:
            channel = "Paid Search"
        elif utm_source and any(keyword in utm_source.lower() for keyword in ['google', 'yahoo']):
            channel = "Paid Search"
        elif not (utm_source or utm_campaign or utm_medium) and referrer_domain:
            channel = "Referral"
        elif not (utm_source or utm_campaign or utm_medium or referrer_domain):
            channel = "Direct"
        
        extracted_data.append((utm_source, utm_campaign, utm_medium, gclid, channel))
    
    return extracted_data

# %% [markdown]
# 
# ### 7. Store Extracted Data in SQLite Database : Stores the extracted data in an SQLite database by creating a new dimension table called `channel` and inserting the data into it.
# 

# %%
def store_extracted_data_in_db(extracted_data, db_file_name):
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            utm_source TEXT,
            utm_campaign TEXT,
            utm_medium TEXT,
            gclid TEXT,
            channel TEXT
        )
    ''')
    cursor.execute('DELETE FROM channel')

    cursor.executemany('''
        INSERT INTO channel (utm_source, utm_campaign, utm_medium, gclid, channel)
        VALUES (?, ?, ?, ?, ?)
    ''', extracted_data)

    conn.commit()
    conn.close()

# %% [markdown]
# 
# ### 8. Create and Query a View: Creates a view to filter out pageviews where the referrer contains the domain "fashion-brand.com" and count the number of pageviews per channel. 
# 

# %%
def create_and_query_view(db_file_name):
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()

    cursor.execute('DROP VIEW IF EXISTS filtered_pageviews')

    cursor.execute('''
        CREATE VIEW filtered_pageviews AS
        SELECT COUNT(*) as pageview_count, c.channel FROM pageviews p, channel c 
        WHERE p.action = 'pageview' AND p.referrer_domain LIKE '%fashion-brand.com%'
        GROUP BY c.channel
    ''')

    conn.close()

# %% [markdown]
# 
# ### 9. Main Function: Executes the ETL pipeline by calling the defined functions in sequence.
# 

# %%
def main():
    logs = fetch_tracking_logs(from_date=from_date, to_date=to_date, offset=0, limit=100000)

    if logs:
        print(logs[:10])
    else:
        print("No logs fetched.")
        return

    parsed_logs = parse_logs(logs)
    store_logs_in_db(parsed_logs, db_file_name)

    extracted_data = extract_query_params(logs)
    store_extracted_data_in_db(extracted_data, db_file_name)

    create_and_query_view(db_file_name)

if __name__ == "__main__":
    main()




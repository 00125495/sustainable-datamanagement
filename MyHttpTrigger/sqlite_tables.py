import sqlite3
import json
import pandas as pd
# from datetime import datetime
import datetime
import pytz
from urllib.parse import urlparse

# Define the SQLite database file path and table name
db_path = 'D:\\Vinoth\\Github\\azure_functions\\MyFunctionProj\\MyHttpTrigger\\access_logs.db'
table_name = 't_access_logs'

# Define the table schema as a dictionary
schema = {
    'time': 'TEXT',
    'resourceId': 'TEXT',
    'category': 'TEXT',
    'operationName': 'TEXT',
    'operationVersion': 'TEXT',
    'schemaVersion': 'TEXT',
    'statusCode': 'INTEGER',
    'statusText': 'TEXT',
    'durationMs': 'INTEGER',
    'callerIpAddress': 'TEXT',
    'correlationId': 'TEXT',
    'identity': 'TEXT',
    'location': 'TEXT',
    'properties': 'TEXT',
    'uri': 'TEXT',
    'protocol': 'TEXT',
    'resourceType': 'TEXT'
}

# Convert the schema dictionary to a string
schema_str = ', '.join([f'{col_name} {col_type}' for col_name, col_type in schema.items()])

# Create a connection to the SQLite database
conn = sqlite3.connect(db_path)

# Create a cursor object
cur = conn.cursor()

# Create the table with the specified schema
cur.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({schema_str})')

# Commit the changes and close the connection
conn.commit()
conn.close()

def insert_data_to_sqlite(db_path: str, table_name: str, data_dict: list, truncate: bool = False):

    # Create a connection to the SQLite database
    conn = sqlite3.connect(db_path)

    # Create a cursor object
    cur = conn.cursor()

    # Truncate the table if specified
    if truncate:
        cur.execute(f'DELETE FROM {table_name};')

    # Define the SQL INSERT statement and parameters
    sql = f"INSERT INTO {table_name} (time, resourceId, category, operationName, operationVersion, schemaVersion, statusCode, statusText, durationMs, callerIpAddress, correlationId, identity, location, properties, uri, protocol, resourceType) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    
    for data in data_dict:
        params = (
            data['time'],
            data['resourceId'],
            data['category'],
            data['operationName'],
            data['operationVersion'],
            data['schemaVersion'],
            data['statusCode'],
            data['statusText'],
            data['durationMs'],
            data['callerIpAddress'],
            data['correlationId'],
            json.dumps(data['identity']),
            data['location'],
            json.dumps(data['properties']),
            data['uri'],
            data['protocol'],
            data['resourceType']
        )

        # Execute the INSERT statement
        cur.execute(sql, params)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


def enrich_table(db_path: str, table_name: str, profile_report: bool = False):
    # Create a connection to the SQLite database
    conn = sqlite3.connect(db_path)

    # Create a cursor object
    cur = conn.cursor()

    # Read the data from the SQLite table into a Pandas DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    # Convert the "time" column to a datetime object
    df['time'] = pd.to_datetime(df['time'])

    # Calculate the difference between the "time" column and the current system time
    now = datetime.datetime.now()

    # get the timezone you want to add to the datetime object
    timezone = pytz.timezone('America/New_York')

    # add the timezone to the datetime object
    systime = timezone.localize(now)

    # derived columns for time lapse in seconds, minutes, hours and days
    df['lapse_in_sec'] = (systime - df['time']).dt.total_seconds()
    df['lapse_in_minutes'] = df['lapse_in_sec'] / 60
    df['lapse_in_hours'] = df['lapse_in_minutes'] / 60
    df['lapse_in_days'] = df['lapse_in_hours'] / 24

    # print(df.head(5).to_string(index=False))
    # print(df.to_string(index=False))

    # Select desired columns and apply filter condition
    df_selected = df.loc[~(df['operationName'] == 'RenewBlobLease'),
                        ['time', 'category', 'operationName',
                        'statusCode', 'statusText', 'durationMs','properties', 'uri', 'lapse_in_sec',
                        'lapse_in_minutes', 'lapse_in_hours', 'lapse_in_days']]

    # Define lambda function to extract components
    def extract_components(row):
        parsed_uri = urlparse(row['uri'])
        prop_dict = json.loads(row.properties)

        return pd.Series({
            'accountName' : prop_dict["accountName"],
            'objectKey': prop_dict["objectKey"],
            'prototype': parsed_uri.scheme,
            'hostname': parsed_uri.hostname,
            'path': parsed_uri.path,
            'port': parsed_uri.port,
            'query': parsed_uri.query,
            'fragment': parsed_uri.fragment,
            'params': parsed_uri.params,
        })
    
    # Apply lambda function to 'uri' column and add new columns to dataframe
    df_selected[['accountName', 'objectKey', 'prototype', 'hostname', 'path', 'port', 'query', 'fragment', 'params']] = df_selected.apply(extract_components, axis=1)

    schema = {
    'time': 'TEXT',
    'category': 'TEXT',
    'operationName': 'TEXT',
    'statusCode': 'INTEGER',
    'statusText': 'TEXT',
    'durationMs': 'INTEGER',
    'accountName': 'TEXT',
    'objectKey': 'TEXT',
    'prototype': 'TEXT',
    'hostname': 'TEXT',
    'path': 'TEXT',
    'port': 'INTEGER',
    'query': 'TEXT',
    'fragment': 'TEXT',
    'params': 'TEXT',
    'lapse_in_sec': 'INTEGER',
    'lapse_in_minutes': 'INTEGER',
    'lapse_in_hours': 'INTEGER',
    'lapse_in_days': 'INTEGER'
    }
    
    # Convert the schema dictionary to a string
    schema_str = ', '.join([f'{col_name} {col_type}' for col_name, col_type in schema.items()])

    # create a new table to store the selected data from the original table
    cur.execute('''DROP TABLE IF EXISTS t_audit_summary_2''')
    cur.execute(f'CREATE TABLE IF NOT EXISTS t_audit_summary_2 ({schema_str})')

    # Insert the selected data into the table
    for row in df_selected.itertuples():
        conn.execute('''INSERT INTO t_audit_summary_2 (time, category, operationName, statusCode, statusText, durationMs, accountName, objectKey, prototype, hostname, path, port, query, fragment, params, lapse_in_sec, lapse_in_minutes, lapse_in_hours, lapse_in_days) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                ((row.time).strftime('%Y-%m-%d %H:%M:%S.%f'), row.category, row.operationName, row.statusCode, row.statusText, row.durationMs, row.accountName, row.objectKey
                 , row.prototype, row.hostname, row.path, row.port, row.query, row.fragment, row.params, row.lapse_in_sec, row.lapse_in_minutes, row.lapse_in_hours, row.lapse_in_days))

    if profile_report:
        # Profile the DataFrame
        import pandas_profiling
        profile = df.profile_report()
        profile.to_file('D:\\Vinoth\\Github\\azure_functions\\MyFunctionProj\\MyHttpTrigger\\my_profile_report.html')

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

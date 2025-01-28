import os
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values

# Logger configuration
logger_config = {
    'host': os.getenv('LOGGER_HOST'),
    'port': os.getenv('LOGGER_PORT'),
    'database': os.getenv('LOGGER_DATABASE'),
    'user': os.getenv('LOGGER_USER'),
    'password': os.getenv('LOGGER_PASSWORD'),
}

# Analytics DB configuration
analytics_config = {
    'host': os.getenv('ANALYTICS_HOST'),
    'port': os.getenv('ANALYTICS_PORT'),
    'dbname': os.getenv('ANALYTICS_DATABASE'),
    'user': os.getenv('ANALYTICS_USER'),
    'password': os.getenv('ANALYTICS_PASSWORD'),
}

# Query to fetch data from Logger
logger_query = """
SELECT
  le.id,
  created,
  lrt.name AS reason,
  lst.name AS source,
  CASE 
    WHEN lst.name = 'ALA' OR user_agent LIKE 'mozilla%' THEN 'Web'
    WHEN lst.name = 'ALA4R' THEN 'SBDI4R-1'
    WHEN lst.name = 'galah' THEN 'galah-R'
    WHEN user_agent LIKE 'galah-python%' THEN 'galah-python'
    ELSE NULL
  END AS client,
  CASE WHEN lrt.name = 'testing' OR user_email IN (
    'manash.shah@nrm.se',
    'manash.shah@gmail.com',
    'mats.bovin@nrm.se',
    'mats.bovin@gmail.com',
    'sbdi4r-test@biodiversitydata.se',
    'aleruete@gmail.com',
    'martinjwestgate@gmail.com') 
    THEN 1 
    ELSE 0 
  END AS is_test,
  sha2(concat('salt', user_email), 256) AS user_key,
  user_agent
FROM
  log_event le
  LEFT JOIN log_source_type lst ON le.log_source_type_id = lst.id
  LEFT JOIN log_reason_type lrt ON le.log_reason_type_id = lrt.id
WHERE
  log_event_type_id = 1002;
"""

# Function to fetch data from Logger
def fetch_data_from_logger():
    connection = mysql.connector.connect(**logger_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(logger_query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

# Function to insert data into PostgreSQL
def insert_data_into_analytics_db(data):
    connection = psycopg2.connect(**analytics_config)

    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE downloads;")
    connection.commit()
    cursor.close()

    insert_query = """
    INSERT INTO downloads (id, created, reason, source, client, is_test, user_key, user_agent)
    VALUES %s
    """
    cursor = connection.cursor()
    values = [
        (
            row['id'],
            row['created'],
            row['reason'],
            row['source'],
            row['client'],
            bool(row['is_test']),
            row['user_key'],
            row['user_agent']
        )
        for row in data
    ]
    execute_values(cursor, insert_query, values)
    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    print("Fetching data from Logger...")
    mysql_data = fetch_data_from_logger()
    print(f"Fetched {len(mysql_data)} rows.")

    print("Inserting data into Analytics-DB...")
    insert_data_into_analytics_db(mysql_data)
    print("Data insertion completed.")

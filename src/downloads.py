import os

import mysql.connector
import psycopg2.extras

db_config = {
    'host': os.getenv('LOGGER_HOST'),
    'port': os.getenv('LOGGER_PORT'),
    'database': os.getenv('LOGGER_DATABASE'),
    'user': os.getenv('LOGGER_USER'),
    'password': os.getenv('LOGGER_PASSWORD'),
}


def _fetch():
    query = """
SELECT
  le.id,
  created,
  lrt.name AS reason,
  lst.name AS source,
  (SELECT SUM(record_count) 
	  FROM log_detail 
	  WHERE log_event_id = le.id 
	  AND entity_uid LIKE 'dr%') AS record_count,
  CASE
    WHEN lst.name = 'ALA'
    OR user_agent LIKE 'mozilla%' THEN 'Web'
    WHEN lst.name = 'ALA4R' THEN 'SBDI4R-1'
    WHEN lst.name = 'galah' THEN 'galah-R'
    WHEN user_agent LIKE 'galah-python%' THEN 'galah-python'
    ELSE NULL
  END AS client,
  CASE
    WHEN lrt.name = 'testing'
    OR user_email IN (
      'manash.shah@nrm.se',
      'manash.shah@gmail.com',
      'mats.bovin@nrm.se',
      'mats.bovin@gmail.com',
      'sbdi4r-test@biodiversitydata.se',
      'aleruete@gmail.com',
      'martinjwestgate@gmail.com'
    ) THEN 1
    ELSE 0
  END AS is_test,
  sha2(concat('""" + os.getenv('SALT') + """', user_email), 256) AS user_key,
  user_agent
FROM
  log_event le
  LEFT JOIN log_source_type lst ON le.log_source_type_id = lst.id
  LEFT JOIN log_reason_type lrt ON le.log_reason_type_id = lrt.id
WHERE
  log_event_type_id = 1002
  AND EXISTS (SELECT 1 FROM log_detail WHERE log_event_id = le.id)
"""

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()

    return results


def _insert(downloads, connection):

    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE download")

    insert_query = """
    INSERT INTO download (id, created, reason, source, record_count, client, is_test, user_key, user_agent)
    VALUES %s
    """
    values = [
        (
            row['id'],
            row['created'],
            row['reason'],
            row['source'],
            row['record_count'], 
            row['client'],
            bool(row['is_test']),
            row['user_key'],
            row['user_agent']
        )
        for row in downloads
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()


def transfer(analytics_conn):
    
    try:
      print("Downloads > fetching", end="")
      result = _fetch()
      print(f" - done, {len(result)} rows", end="")

      print(" > inserting", end="")
      _insert(result, analytics_conn)
      print(" - done")
    
    except Exception as e:
        print(f"\nDownloads failed: {e}")
        analytics_conn.rollback()

import os

import psycopg2
import psycopg2.extras

db_config = {
    'host': os.getenv('SPATIAL_HOST'),
    'port': os.getenv('SPATIAL_PORT'),
    'database': os.getenv('SPATIAL_DATABASE'),
    'user': os.getenv('SPATIAL_USER'),
    'password': os.getenv('SPATIAL_PASSWORD'),
}

def _fetch():
    query = """
SELECT 
  id, 
  created, 
  name, 
  message,
  CASE
    WHEN user_id = 'null' THEN NULL
    ELSE user_id::INT
  END AS user_id 
FROM task 
"""
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()

    return results

def _insert(downloads, connection):

    cursor = connection.cursor()
    
    cursor.execute("TRUNCATE TABLE spatial_task")

    insert_query = """
    INSERT INTO spatial_task (id, created, name, message, user_id)
    VALUES %s
    """
    values = [
        (
            row['id'],
            row['created'],
            row['name'],
            row['message'],
            row['user_id']
        )
        for row in downloads
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()

def transfer(analytics_conn):
    
    try:
      print("Spatial portal tasks > fetching", end="")
      result = _fetch()
      print(f" - done, {len(result)} rows", end="")

      print(" > inserting", end="")
      _insert(result, analytics_conn)
      print(" - done")
    
    except Exception as e:
        print(f"\nSpatial portal tasks failed: {e}")
        analytics_conn.rollback()

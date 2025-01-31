import os
import mysql.connector
import psycopg2
import psycopg2.extras

analytics_config = {
    'host': os.getenv('ANALYTICS_HOST'),
    'port': os.getenv('ANALYTICS_PORT'),
    'dbname': os.getenv('ANALYTICS_DATABASE'),
    'user': os.getenv('ANALYTICS_USER'),
    'password': os.getenv('ANALYTICS_PASSWORD'),
}

logger_config = {
    'host': os.getenv('LOGGER_HOST'),
    'port': os.getenv('LOGGER_PORT'),
    'database': os.getenv('LOGGER_DATABASE'),
    'user': os.getenv('LOGGER_USER'),
    'password': os.getenv('LOGGER_PASSWORD'),
}

user_config = {
    'host': os.getenv('CAS_HOST'),
    'port': os.getenv('CAS_PORT'),
    'database': os.getenv('CAS_DATABASE'),
    'user': os.getenv('CAS_USER'),
    'password': os.getenv('CAS_PASSWORD'),
}


def fetch_downloads_from_logger():
    downloads_query = """
SELECT
  le.id,
  created,
  lrt.name AS reason,
  lst.name AS source,
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
  log_event_type_id = 1002;
"""

    connection = mysql.connector.connect(**logger_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(downloads_query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results


def insert_downloads_into_analytics_db(downloads):

    connection = psycopg2.connect(**analytics_config)
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE download;")

    insert_query = """
    INSERT INTO download (id, created, reason, source, client, is_test, user_key, user_agent)
    VALUES %s
    """
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
        for row in downloads
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()
    connection.close()


def fetch_users():
    query = """
SELECT
  u.userid AS id,
  sha2(concat('""" + os.getenv('SALT') + """', username), 256) AS user_key,
  date_created,
  last_updated,
  last_login,
  pc.value AS country,
  po.value AS organisation
FROM
  users u
  LEFT JOIN profiles pc ON u.userid = pc.userid AND pc.property = 'country'
  LEFT JOIN profiles po ON u.userid = po.userid AND po.property = 'organisation';
"""

    connection = mysql.connector.connect(**user_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results


def insert_users(users):

    connection = psycopg2.connect(**analytics_config)
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE sbdi_user;")

    insert_query = """
    INSERT INTO sbdi_user (id, user_key, date_created, last_updated, last_login, country, organisation)
    VALUES %s
    """
    values = [
        (
            row['id'],
            row['user_key'],
            row['date_created'],
            row['last_updated'],
            row['last_login'],
            row['country'],
            row['organisation']
        )
        for row in users
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()
    connection.close()
   
if __name__ == "__main__":
    print("Fetching downloads from Logger...")
    users = fetch_downloads_from_logger()
    print(f"Fetched {len(users)} rows")

    print("Inserting downloads into Analytics-DB...")
    insert_downloads_into_analytics_db(users)
    print("Done")

    print("Fetching users")
    users = fetch_users()
    print(f"Fetched {len(users)} rows")

    print("Inserting users...")
    insert_users(users)
    print("Done")

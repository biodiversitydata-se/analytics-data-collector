import os

import mysql.connector
import psycopg2.extras

db_config = {
    'host': os.getenv('CAS_HOST'),
    'port': os.getenv('CAS_PORT'),
    'database': os.getenv('CAS_DATABASE'),
    'user': os.getenv('CAS_USER'),
    'password': os.getenv('CAS_PASSWORD'),
}


def _fetch():
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
    LEFT JOIN profiles po ON u.userid = po.userid AND po.property = 'organisation'
"""

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()

    return results


def _insert(users, connection):

    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE \"user\"")

    insert_query = """
    INSERT INTO "user" (id, user_key, date_created, last_updated, last_login, country, organisation)
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


def transfer(analytics_conn):

    try:
        print("Users > fetching", end="")
        result = _fetch()
        print(f" - done, {len(result)} rows", end="")

        print(" > inserting", end="")
        _insert(result, analytics_conn)
        print(" - done")
    
    except Exception as e:
        print(f"\nUsers failed: {e}")
        analytics_conn.rollback()

import datetime
import os
import hashlib

import psycopg2.extras
import pymongo


def _fetch():
    client = pymongo.MongoClient(
        host=os.getenv('CAS_AUDIT_HOST'),
        port=int(os.getenv('CAS_AUDIT_PORT')),
        username=os.getenv('CAS_AUDIT_USER'),
        password=os.getenv('CAS_AUDIT_PASSWORD'),
        authSource=os.getenv('CAS_AUDIT_DATABASE')
    )
    db = client[os.getenv('CAS_AUDIT_DATABASE')]
    collection = db['MongoDbCasAuditRepository']
    
    query = {"actionPerformed": "AUTHENTICATION_SUCCESS", "principal": {"$ne": "mats.bovin@nrm.se"}}
    projection = {"principal": 1, "whenActionWasPerformed": 1, "_id": 0}
    
    result = list(collection.find(query, projection))
    client.close()

    return result


def _insert(logins, connection):
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE login")

    insert_query = """
    INSERT INTO login (user_key, login_time)
    VALUES %s
    """
    values = [
        (
            hashlib.sha256((os.getenv('SALT') + row['principal']).encode('utf-8')).hexdigest(),
            row['whenActionWasPerformed'].replace(tzinfo=datetime.timezone.utc)
        )
        for row in logins
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()


def transfer(analytics_conn):

    try:
        print("Logins > fetching", end="")
        logins = _fetch()
        print(f" - done, {len(logins)} rows", end="")

        print(f" > inserting", end="")
        _insert(logins, analytics_conn)
        print(" - done")
    
    except Exception as e:
        print(f"\nLogins failed: {e}")
        analytics_conn.rollback()

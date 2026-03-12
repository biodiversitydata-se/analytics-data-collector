import datetime
import hashlib
import os
import re

import psycopg2.extras
import pymongo


def _get_last_transfer(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT MAX(login_time) FROM login")
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result and result[0] else datetime.datetime.min


def _fetch(last_transfer):
    client = pymongo.MongoClient(
        host=os.getenv('CAS_AUDIT_HOST'),
        port=int(os.getenv('CAS_AUDIT_PORT')),
        username=os.getenv('CAS_AUDIT_USER'),
        password=os.getenv('CAS_AUDIT_PASSWORD'),
        authSource=os.getenv('CAS_AUDIT_DATABASE')
    )
    db = client[os.getenv('CAS_AUDIT_DATABASE')]
    collection = db['MongoDbCasAuditRepository']
    
    # Fetch all relevant audit events from MongoDB
    logins = list(collection
                    .find({"actionPerformed": "AUTHENTICATION_SUCCESS", 
                            "whenActionWasPerformed": {"$gt": last_transfer},
                            "principal": {"$nin": ["mats.bovin@nrm.se", 
                                                   "mats.bovin@gmail.com", 
                                                   "manash.shah@nrm.se"]}})
                    .sort("whenActionWasPerformed", 1))
    oauth2_events = list(collection
                            .find({"actionPerformed": "OAUTH2_USER_PROFILE_CREATED",
                                   "whenActionWasPerformed": {"$gt": last_transfer}})
                            .sort("whenActionWasPerformed", 1))
    service_ticket_events = list(collection
                                    .find({"actionPerformed": "SERVICE_TICKET_VALIDATE_SUCCESS",
                                           "whenActionWasPerformed": {"$gt": last_transfer}})
                                    .sort("whenActionWasPerformed", 1))

    client.close()

    # Determine the service for each login by matching it with OAUTH2 events or service ticket events
    for login in logins:
        # Try to find a matching OAUTH2 event for the login
        oauth2_event = next(
            (
                event for event in oauth2_events 
                    if event['principal'] == login['principal'] 
                    and event['whenActionWasPerformed'] > login['whenActionWasPerformed']
                    and event['whenActionWasPerformed'] - login['whenActionWasPerformed'] < datetime.timedelta(seconds=2)
            ), None)
        if oauth2_event:
            login['service'] = re.search(r'service=https?://(.+?)/callback', 
                                         oauth2_event['resourceOperatedUpon']).group(1)
        else:
            # If no OAUTH2 event is found, try to find a matching service ticket event
            ticket_event = next(
                (
                    event for event in service_ticket_events 
                        if event['principal'] == login['principal']
                        and event['whenActionWasPerformed'] > login['whenActionWasPerformed']
                        and event['whenActionWasPerformed'] - login['whenActionWasPerformed'] < datetime.timedelta(seconds=2)
                ), None)
            if ticket_event:
                login['service'] = re.search(r'service=https?://(.+?)/', 
                                             ticket_event['resourceOperatedUpon']).group(1)
            # If no matching event is found, default to 'auth.biodiversitydata.se'
            else:
                login['service'] = 'auth.biodiversitydata.se'

    # Exclude logins with a service that contains a colon because they are 
    # likely to be test or internal services (eg localhost:8080, devt.biodiversitydata.se:8087)
    return [login for login in logins if not ':' in login['service']]


def _insert(logins, connection):
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO login (user_key, service, login_time)
    VALUES %s
    """
    values = [
        (
            hashlib.sha256((os.getenv('SALT') + row['principal']).encode('utf-8')).hexdigest(),
            row['service'],
            row['whenActionWasPerformed'].replace(tzinfo=datetime.timezone.utc)
        )
        for row in logins
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()


def transfer(analytics_conn):

    try:
        last_transfer = _get_last_transfer(analytics_conn)

        print(f"Logins > fetching from {last_transfer.isoformat()}", end="")
        logins = _fetch(last_transfer)
        print(f" - done, {len(logins)} rows", end="")

        print(f" > inserting", end="")
        _insert(logins, analytics_conn)
        print(" - done")
    
    except Exception as e:
        print(f"\nLogins failed: {e}")
        analytics_conn.rollback()

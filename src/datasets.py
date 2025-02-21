import os
import re

import mysql.connector
import psycopg2.extras
import requests

db_config = {
    'host': os.getenv('COLLECTORY_HOST'),
    'port': os.getenv('COLLECTORY_PORT'),
    'database': os.getenv('COLLECTORY_DATABASE'),
    'user': os.getenv('COLLECTORY_USER'),
    'password': os.getenv('COLLECTORY_PASSWORD'),
}


def _fetch_record_counts():

    query_params = {
        'facets': 'data_resource_uid',
        'flmit': '-1',
        'count': '0',
    }
    response = requests.get(f"{os.getenv('BIOCACHE_HOST')}/ws/occurrences/facets", 
                            params=query_params, 
                            timeout=(10, 30))
    response.raise_for_status()

    result = {}
    uid_pattern = re.compile(r'data_resource_uid:"(dr\d+)"')
    for item in response.json()[0]['fieldResult']:
        uid = uid_pattern.findall(item['fq'])[0]
        result[uid] = item['count']

    return result


def _fetch_media_counts():

    query_params = {
        'facet': 'dataResourceUid',
    }
    response = requests.get(f"{os.getenv('IMAGE_SERVICE_HOST')}/ws/facet", 
                            params=query_params, 
                            timeout=(10, 30))
    response.raise_for_status()

    return response.json()['dataResourceUid']


def _fetch_from_db():
    query = """
SELECT
  dr.id,
  dr.uid,
  dr.name,
  dr.resource_type,
  dp.name AS data_provider,
  i.name AS institution,
  dr.date_created,
  dr.data_currency
FROM
  data_resource dr
  LEFT JOIN data_provider dp ON dp.id = dr.data_provider_id
  LEFT JOIN institution i ON i.id = dr.institution_id;
"""

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()

    return results


def _fetch():

    record_counts =_fetch_record_counts()
    print(record_counts)
    print()
    media_counts = _fetch_media_counts()
    print(media_counts)
    result = _fetch_from_db()
    return result


def _insert(datasets, connection):

    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE dataset")

    insert_query = """
    INSERT INTO dataset (id, uid, name, resource_type, data_provider, institution, date_created, data_currency)
    VALUES %s
    """
    values = [
        (
            row['id'],
            row['uid'],
            row['name'],
            row['resource_type'],
            row['data_provider'],
            row['institution'],
            row['date_created'],
            row['data_currency']
        )
        for row in datasets
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()


def transfer(analytics_conn):

    try:
        print("Datasets > fetching", end="")
        result = _fetch()
        print(f" - done, {len(result)} rows", end="")

        print(" > inserting", end="")
        _insert(result, analytics_conn)
        print(" - done")
    
    except Exception as e:
        print(f"\nDatasets failed: {e}")

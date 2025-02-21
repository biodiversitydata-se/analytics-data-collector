import datetime
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
        'flimit': '-1',
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


def _fetch_datasets():
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

    record_counts = _fetch_record_counts()
    media_counts = _fetch_media_counts()

    counts = {
        uid: {
              'record_count': record_count, 
              'media_count': media_counts.get(uid, 0)
             } 
        for (uid, record_count) in record_counts.items() 
    }
    
    datasets = _fetch_datasets()

    return datasets, counts


def _insert(datasets, counts, save_snapshot, connection):

    cursor = connection.cursor()

    # Datasets
    cursor.execute("TRUNCATE TABLE dataset")

    insert_query = """
    INSERT INTO dataset (
        id,
        uid,
        name,
        resource_type,
        data_provider,
        institution,
        date_created,
        data_currency,
        records,
        media)
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
            row['data_currency'],
            counts.get(row['uid'], {}).get('record_count', 0),
            counts.get(row['uid'], {}).get('media_count', 0),
        )
        for row in datasets
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    # Dataset snapshot
    if save_snapshot:
        insert_query = "INSERT INTO dataset_snapshot (uid, snapshot_date, records, media) VALUES %s"
        values = [
            (
                uid,
                datetime.date.today() - datetime.timedelta(days=1),
                item['record_count'],
                item['media_count'],
            )
            for (uid, item) in counts.items()
        ]
        psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()


def transfer(analytics_conn):

    try:
        print("Datasets > fetching", end="")
        datasets, counts = _fetch()
        print(f" - done, {len(datasets)} rows", end="")

        # Save snapshot on the first day of the month
        save_snapshot = datetime.date.today().day == 1

        print(f" > inserting, snapshot: {save_snapshot}", end="")
        _insert(datasets, counts, save_snapshot, analytics_conn)
        print(" - done")
    
    except Exception as e:
        print(f"\nDatasets failed: {e}")

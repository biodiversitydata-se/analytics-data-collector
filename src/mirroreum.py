import datetime
import os

import psycopg2.extras
import requests

test_ips = [
    '130.242.24.193', # NRM
    '92.244.21.152'   # mats@home
    ]


def _fetch():

    query_params = {
        'query': '{service="proxy_proxy"} |= ` "mirroreum.biodiversitydata.se" ` |= `POST /rpc/client_init`',
        'since': '24h',
        'direction': 'FORWARD',
    }
    response = requests.get(f"{os.getenv('LOKI_HOST')}/loki/api/v1/query_range", 
                            params=query_params, 
                            timeout=(10, 30))
    response.raise_for_status()

    json_response = response.json()
    if json_response['status'] != 'success':
        raise Exception(f"Failed to fetch data from Loki: {json_response}")
    
    logins = []
    for item in json_response['data']['result']:
        value = item['values'][0]
        ip = value[1].split()[0]
        logins.append({
            'login_at': datetime.datetime.fromtimestamp(int(value[0][:10])),
            'ip': ip,
            'is_test': ip in test_ips,
        })

    return logins


def _insert(logins, connection):

    cursor = connection.cursor()

    insert_query = """
    INSERT INTO "mirroreum_login" (login_at, ip, is_test)
    VALUES %s
    """
    values = [
        (
            row['login_at'],
            row['ip'],
            row['is_test']
        )
        for row in logins
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    connection.commit()
    cursor.close()


def transfer(analytics_conn):

    try:
        print("Mirroreum > fetching", end="")
        logins = _fetch()
        print(f" - done, {len(logins)} logins", end="")

        print(" > inserting", end="")
        _insert(logins, analytics_conn)
        print(" - done")
    
    except Exception as e:
        print(f"\nMirroreum failed: {e}")
        analytics_conn.rollback()

import datetime
import os

import psycopg2.extras
import requests


def _make_api_request(method, additional_query_params = {}):
    query_params = {
        'module': 'API',
        'method': method,
        'format': 'JSON',
    }
    query_params.update(additional_query_params)

    payload = {
        'token_auth': os.getenv('MATOMO_TOKEN'),
    }

    response = requests.post(f"{os.getenv('MATOMO_HOST')}/index.php", 
                             params=query_params, 
                             data=payload,
                             timeout=(10, 30))
    response.raise_for_status()

    return response.json()


def _fetch():
    sites = _make_api_request('SitesManager.getAllSites')
    visits = _make_api_request('VisitsSummary.get', { 'idSite': 'all', 'period': 'day', 'date': 'yesterday' })
    actions = _make_api_request('Actions.get', { 'idSite': 'all', 'period': 'day', 'date': 'yesterday' })

    return sites, visits, actions


def _insert(sites, visits, actions, connection):

    cursor = connection.cursor()

    # sites
    cursor.execute("TRUNCATE TABLE site")

    insert_query = """
    INSERT INTO site (id, name)
    VALUES %s
    """
    values = [
        (
            site['idsite'],
            site['name'],
        )
        for site in sites
    ]
    psycopg2.extras.execute_values(cursor, insert_query, values)

    # visits and actions
    insert_query = """
    INSERT INTO visit (
        date,
        site_id,
        unique_visitors,
        visits,
        actions,
        bounce_count,
        sum_visit_length,
        pageviews,
        unique_pageviews,
        downloads,
        unique_downloads,
        outlinks,
        unique_outlinks,
        searches,
        keywords
    )
    VALUES %s
    """

    values = []
    for site_id, visit in visits.items():
        action = actions[site_id]
        values.append((
            (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
            site_id,
            visit['nb_uniq_visitors'] if isinstance(visit, dict) else 0,
            visit['nb_visits'] if isinstance(visit, dict) else 0,
            visit['nb_actions'] if isinstance(visit, dict) else 0,
            visit['bounce_count'] if isinstance(visit, dict) else 0,
            visit['sum_visit_length'] if isinstance(visit, dict) else 0,
            action['nb_pageviews'] if isinstance(action, dict) else 0,
            action['nb_uniq_pageviews'] if isinstance(action, dict) else 0,
            action['nb_downloads'] if isinstance(action, dict) else 0,
            action['nb_uniq_downloads'] if isinstance(action, dict) else 0,
            action['nb_outlinks'] if isinstance(action, dict) else 0,
            action['nb_uniq_outlinks'] if isinstance(action, dict) else 0,
            action['nb_searches'] if isinstance(action, dict) else 0,
            action['nb_keywords'] if isinstance(action, dict) else 0,
        ))
    try:
        psycopg2.extras.execute_values(cursor, insert_query, values)
    except psycopg2.errors.UniqueViolation as e:
        print(f" {e}", end="")

    connection.commit()
    cursor.close()


def transfer(analytics_conn):

    try:
        print("Visits > fetching", end="")
        sites, visits, actions = _fetch()
        print(f" - done, {len(sites)} sites", end="")

        print(" > inserting", end="")
        _insert(sites, visits, actions, analytics_conn)
        print(" - done")
    
    except Exception as e:
        print(f"\nVisits failed: {e}")

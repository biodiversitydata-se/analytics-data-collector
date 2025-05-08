import os

import psycopg2

import downloads
import users
import visits
import datasets

analytics_config = {
    'host': os.getenv('ANALYTICS_HOST'),
    'port': os.getenv('ANALYTICS_PORT'),
    'dbname': os.getenv('ANALYTICS_DATABASE'),
    'user': os.getenv('ANALYTICS_USER'),
    'password': os.getenv('ANALYTICS_PASSWORD'),
}


if __name__ == "__main__":

    analytics_conn = psycopg2.connect(**analytics_config)

    downloads.transfer(analytics_conn)
    users.transfer(analytics_conn)
    visits.transfer(analytics_conn)
    datasets.transfer(analytics_conn)

    print("=== Done ===")

    analytics_conn.close()

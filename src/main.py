import os
import sys

import psycopg2

import downloads
import users
import visits
import datasets
import spatial_portal

analytics_config = {
    'host': os.getenv('ANALYTICS_HOST'),
    'port': os.getenv('ANALYTICS_PORT'),
    'dbname': os.getenv('ANALYTICS_DATABASE'),
    'user': os.getenv('ANALYTICS_USER'),
    'password': os.getenv('ANALYTICS_PASSWORD'),
}


def run_module(module_name):
    return len(sys.argv) <= 1 or module_name == sys.argv[1]


if __name__ == "__main__":

    analytics_conn = psycopg2.connect(**analytics_config)

    if run_module('downloads'):
        downloads.transfer(analytics_conn)
    if run_module('users'):
        users.transfer(analytics_conn)
    if run_module('visits'):
        visits.transfer(analytics_conn)
    if run_module('datasets'):
        datasets.transfer(analytics_conn)
    if run_module('spatial_portal'):
        spatial_portal.transfer(analytics_conn)

    print("=== Done ===")

    analytics_conn.close()

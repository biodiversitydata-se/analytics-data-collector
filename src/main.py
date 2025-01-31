import os

import users
import downloads

analytics_config = {
    'host': os.getenv('ANALYTICS_HOST'),
    'port': os.getenv('ANALYTICS_PORT'),
    'dbname': os.getenv('ANALYTICS_DATABASE'),
    'user': os.getenv('ANALYTICS_USER'),
    'password': os.getenv('ANALYTICS_PASSWORD'),
}


if __name__ == "__main__":
    print("Fetching downloads from Logger...")
    result = downloads.fetch_downloads_from_logger()
    print(f"Fetched {len(result)} rows")

    print("Inserting downloads into Analytics-DB...")
    downloads.insert_downloads_into_analytics_db(result, analytics_config)
    print("Done")

    print("Fetching users")
    result = users.fetch_users()
    print(f"Fetched {len(result)} rows")

    print("Inserting users...")
    users.insert_users(result, analytics_config)
    print("Done")

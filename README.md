# Analytics-data-collector

App for collecting and aggregating data from various sources into an analytics database.

## Usage

To run locally you need to:
1. Start the analytics database: `make start-db`
1. Build the docker image: `make build`
1. Start any other databases that you will be reading from (eg Logger and CAS)

Run the app using docker:
```
make run
```

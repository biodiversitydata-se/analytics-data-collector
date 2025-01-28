run:
	docker run --rm --env-file env --name analytics-data-collector analytics-data-collector

build:
	docker build -t analytics-data-collector .

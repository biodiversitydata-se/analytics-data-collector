run:
	docker run --rm --env-file env --name analytics-data-collector analytics-data-collector

build:
	docker build -t analytics-data-collector .

start-db:
	docker compose -f analytics-db/docker-compose.yml up -d

stop-db:
	docker compose -f analytics-db/docker-compose.yml down

release:
	../sbdi-install/utils/make-release.sh

SHELL := /bin/bash
PWD := $(shell pwd)

start-app:
	docker-compose up -d --scale worker-station=3 --scale worker-trip=3 --scale worker-weather=3 --scale distributor=3
.PHONY: start-app

stop-app:
	docker-compose down
.PHONY: stop-app

make-me-anew:
	docker-compose up -d --force-recreate
.PHONY: make-me-anew

rebuild:
	docker-compose build --no-cache
.PHONY: rebuild

logs:
	docker-compose logs -f
.PHONY: logs

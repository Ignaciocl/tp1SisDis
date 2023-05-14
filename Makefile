SHELL := /bin/bash
PWD := $(shell pwd)

start-app:
	docker-compose up -d
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

regenerate-docker:
	python3 addFields.py 3 3 3 3
.PHONY: regenerate-docker

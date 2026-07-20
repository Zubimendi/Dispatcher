.PHONY: up down migrate api worker reconciler test lint tidy

up:
	docker compose up -d
	@echo "Waiting for postgres..."
	@until docker compose exec -T postgres pg_isready -U dispatcher >/dev/null 2>&1; do sleep 1; done
	$(MAKE) migrate

down:
	docker compose down -v

migrate:
	docker compose exec -T postgres psql -U dispatcher -d dispatcher < internal/db/migrations/0001_init.sql

api:
	go run ./cmd/api

worker:
	go run ./cmd/worker

reconciler:
	go run ./cmd/reconciler

test:
	go test ./... -v -race -cover

lint:
	go vet ./...

tidy:
	go mod tidy

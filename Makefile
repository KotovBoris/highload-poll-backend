# ===== highload-poll-backend =====

GO ?= go
COMPOSE ?= docker compose
BIN_DIR := bin

.PHONY: help
help: ## Показать список целей
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Собрать все бинарники локально
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/api ./cmd/api
	$(GO) build -o $(BIN_DIR)/consumer ./cmd/consumer
	$(GO) build -o $(BIN_DIR)/results ./cmd/results
	$(GO) build -o $(BIN_DIR)/loadtest ./loadtest

.PHONY: fmt
fmt: ## gofmt
	$(GO) fmt ./...

.PHONY: vet
vet: ## go vet
	$(GO) vet ./...

.PHONY: test
test: ## Все тесты (с -race)
	$(GO) test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Интеграционные тесты (моки, без внешних сервисов)
	$(GO) test -race -count=1 ./internal/api/... ./internal/results/... ./internal/consumer/...

.PHONY: up
up: ## Поднять весь стек в докере (сборка + запуск)
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Остановить стек
	$(COMPOSE) down

.PHONY: clean
clean: ## Остановить стек и удалить тома
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Логи всех сервисов
	$(COMPOSE) logs -f

.PHONY: ps
ps: ## Статус контейнеров
	$(COMPOSE) ps

.PHONY: e2e
e2e: ## Полный e2e-прогон (поднимает стек, проверяет ручки) — см. scripts/e2e.sh
	./scripts/e2e.sh

.PHONY: loadtest
loadtest: ## Нагрузочный тест на Go против поднятого стека
	$(GO) run ./loadtest -base-url http://localhost:8080 -admin-url http://localhost:8081

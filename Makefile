.PHONY: help start stop restart logs clean build test

help:
	@echo "ICO Analytics Pipeline - Available Commands:"
	@echo ""
	@echo "  start         Start all services"
	@echo "  stop          Stop all services"  
	@echo "  restart       Restart all services"
	@echo "  logs          Follow logs from all services"
	@echo "  clean         Clean up containers and volumes"
	@echo "  build         Build custom Docker images"
	@echo "  test          Run integration tests"
	@echo "  status        Check service status"
	@echo "  kafka-topics  List Kafka topics"
	@echo "  flink-jobs    Show Flink jobs"
	@echo ""

start:
	@echo "Starting ICO Analytics Pipeline..."
	@cp -n .env.example .env || true
	@docker-compose up -d
	@echo "Services starting... Check status with 'make status'"

stop:
	@echo "Stopping ICO Analytics Pipeline..."
	@docker-compose down

restart:
	@echo "Restarting ICO Analytics Pipeline..."
	@docker-compose restart

logs:
	@docker-compose logs -f

clean:
	@echo "Cleaning up containers and volumes..."
	@docker-compose down -v --remove-orphans
	@docker system prune -f

build:
	@echo "Building custom images..."
	@docker-compose build --no-cache

test:
	@echo "Running integration tests..."
	@python tests/integration_test.py

status:
	@echo "Service Status:"
	@echo "==============="
	@docker-compose ps

kafka-topics:
	@echo "Kafka Topics:"
	@echo "============="
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

flink-jobs:
	@echo "Flink Jobs:"
	@echo "==========="
	@curl -s http://localhost:8082/jobs | python -m json.tool

scale-up:
	@echo "Scaling up for high throughput..."
	@docker-compose up -d --scale flink-taskmanager=4
	@echo "EVENTS_PER_MINUTE=2000" >> .env

scale-down:
	@echo "Scaling down for development..."
	@docker-compose up -d --scale flink-taskmanager=1
	@sed -i 's/EVENTS_PER_MINUTE=.*/EVENTS_PER_MINUTE=100/' .env

quick-start:
	@echo "🚀 Quick Start - ICO Analytics Pipeline"
	@echo "======================================"
	@make start
	@echo ""
	@echo "⏳ Waiting for services to start (60 seconds)..."
	@sleep 60
	@echo ""
	@echo "🎉 Pipeline is ready!"
	@echo ""
	@echo "📊 Access your dashboards:"
	@echo "  • Grafana:    http://localhost:3000 (admin/admin)"
	@echo "  • Kafka UI:   http://localhost:8080"
	@echo "  • Flink UI:   http://localhost:8082"
	@echo ""
	@echo "📈 Check status: make status"
	@echo "📋 View logs:    make logs"
.PHONY: help setup test lint format clean docker-up docker-down docker-logs spark-streaming kafka-producer ml-pipeline validate-workflows

# Default target
help:
	@echo "Media Publishing - CI/CD Makefile"
	@echo ""
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies and setup environment"
	@echo "  make test           - Run all tests"
	@echo "  make lint           - Run linting checks"
	@echo "  make format         - Format code with black and isort"
	@echo "  make docker-up      - Start all Docker services"
	@echo "  make docker-down    - Stop all Docker services"
	@echo "  make docker-logs    - Show Docker logs"
	@echo "  make kafka-producer - Run Kafka producer"
	@echo "  make spark-streaming - Run Spark streaming job"
	@echo "  make ml-pipeline    - Run complete ML pipeline"
	@echo "  make validate-workflows - Validate GitHub Actions workflows locally"
	@echo "  make clean          - Clean temporary files"

# Setup environment
setup:
	@echo "Setting up environment..."
	pip install -r requirements.txt 2>/dev/null || echo "No requirements.txt found"
	pip install -r ml/requirements_ml.txt
	pip install black isort flake8 pylint pytest pytest-cov mypy
	@echo "✓ Setup complete"

# Run tests
test:
	@echo "Running tests..."
	pytest tests/ -v --cov=. --cov-report=html --cov-report=term
	@echo "✓ Tests complete"

# Linting
lint:
	@echo "Running linting checks..."
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
	pylint kafka_producer.py spark_streaming.py run_ml.py ml/*.py || true
	@echo "✓ Linting complete"

# Format code
format:
	@echo "Formatting code..."
	black kafka_producer.py spark_streaming.py run_ml.py ml/*.py
	isort kafka_producer.py spark_streaming.py run_ml.py ml/*.py
	@echo "✓ Formatting complete"

# Docker commands
docker-up:
	@echo "Starting Docker services..."
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	sleep 10
	docker-compose ps
	@echo "✓ Docker services started"

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down
	@echo "✓ Docker services stopped"

docker-logs:
	docker-compose logs -f

docker-restart:
	docker-compose restart

# Run services
kafka-producer:
	@echo "Starting Kafka producer..."
	python kafka_producer.py

spark-streaming:
	@echo "Starting Spark streaming..."
	python spark_streaming.py

ml-pipeline:
	@echo "Running ML pipeline..."
	python run_ml.py

# Clean temporary files
clean:
	@echo "Cleaning temporary files..."
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -r {} + 2>/dev/null || true
	rm -rf .pytest_cache/ .coverage htmlcov/ 2>/dev/null || true
	rm -rf checkpoint/ spark-warehouse/ metastore_db/ 2>/dev/null || true
	@echo "✓ Clean complete"

# CI/CD specific commands
ci-test:
	@echo "Running CI tests..."
	pytest tests/ -v --cov=. --cov-report=xml --cov-report=term
	@echo "✓ CI tests complete"

ci-lint:
	@echo "Running CI linting..."
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --max-line-length=127
	black --check kafka_producer.py spark_streaming.py run_ml.py ml/*.py
	isort --check-only kafka_producer.py spark_streaming.py run_ml.py ml/*.py
	@echo "✓ CI linting complete"

ci-format-check:
	@echo "Checking code formatting..."
	black --check kafka_producer.py spark_streaming.py run_ml.py ml/*.py
	isort --check-only kafka_producer.py spark_streaming.py run_ml.py ml/*.py
	@echo "✓ Format check complete"

# Integration tests
test-integration:
	@echo "Running integration tests..."
	pytest tests/integration/ -v -m integration
	@echo "✓ Integration tests complete"

# Check service health
health-check:
	@echo "Checking service health..."
	@docker-compose ps | grep -q "Up" || (echo "✗ Services not running" && exit 1)
	@python test_clickhouse_connection.py || (echo "✗ ClickHouse not accessible" && exit 1)
	@echo "✓ All services healthy"

# Validate GitHub Actions workflows locally
validate-workflows:
	@echo "Validating GitHub Actions workflows..."
	@python validate_workflows.py
	@echo "✓ Workflow validation complete"


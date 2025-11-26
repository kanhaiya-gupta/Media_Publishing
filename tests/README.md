# Tests for OwnLens Media Publishing Platform

This directory contains comprehensive tests for the OwnLens Media Publishing Platform, covering ETL pipelines, ML workflows, and Airflow orchestration.

## Test Structure

```
tests/
├── unit/                          # Unit tests (fast, isolated)
│   ├── etl/
│   │   ├── extractors/
│   │   ├── transformers/
│   │   ├── loaders/
│   │   └── utils/
│   ├── ml/
│   │   └── orchestration/
│   └── airflow/
│       └── operators/
│
├── integration/                   # Integration tests (moderate speed)
│   └── etl/
│
├── e2e/                           # End-to-end tests (slower, realistic)
│   └── test_etl_pipeline_e2e.py
│
├── fixtures/                      # Test fixtures and data
│
├── conftest.py                     # Pytest configuration and fixtures
└── requirements-test.txt           # Test dependencies
```

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test Types

```bash
# Unit tests only
pytest -m unit

# Integration tests only
pytest -m integration

# E2E tests only
pytest -m e2e

# ML tests only
pytest -m ml
```

### Run Tests with Coverage

```bash
pytest --cov=. --cov-report=html
```

### Run Tests in Parallel

```bash
pytest -n auto
```

## Test Dependencies

Install test dependencies:

```bash
pip install -r tests/requirements-test.txt
```

## Test Markers

- `@pytest.mark.unit` - Unit tests (fast, isolated)
- `@pytest.mark.integration` - Integration tests (require services)
- `@pytest.mark.e2e` - End-to-end tests (require full environment)
- `@pytest.mark.slow` - Slow running tests
- `@pytest.mark.ml` - ML pipeline tests

## Fixtures

Common fixtures are defined in `conftest.py`:

- `spark_session` - Test Spark session
- `test_config` - Test ETL configuration
- `mock_spark_session` - Mock Spark session for unit tests
- `temp_dir` - Temporary directory for test files
- `sample_data_dir` - Directory for sample test data

## Coverage Goals

- Overall: 80%
- ETL Orchestration: 90%
- ML Orchestration: 90%
- Airflow DAGs: 85%
- Extractors: 85%
- Transformers: 80%
- Loaders: 85%

## Best Practices

1. **Test Isolation**: Each test should be independent
2. **Fast Tests**: Unit tests should run quickly (< 1 second)
3. **Realistic Mocks**: Use realistic mock data
4. **Clear Assertions**: Use specific assertions
5. **Test Edge Cases**: Test both positive and negative cases

## CI/CD Integration

Tests are automatically run in CI/CD pipeline:

- Pre-commit: Unit tests only
- Pull Request: Unit + Integration tests
- Merge to Main: Full test suite (unit + integration + E2E)





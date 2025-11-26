# Performance Tests

## Overview

Performance tests for the OwnLens Media Publishing Platform, including:
- Throughput benchmarks
- Latency benchmarks
- Memory profiling
- Load tests
- Stress tests

## Test Files

1. **test_etl_performance.py** - ETL component performance tests
2. **test_ml_performance.py** - ML component performance tests
3. **test_airflow_performance.py** - Airflow component performance tests
4. **test_memory_profiling.py** - Memory profiling tests
5. **test_throughput_benchmarks.py** - Throughput benchmark tests
6. **test_latency_benchmarks.py** - Latency benchmark tests

## Running Performance Tests

### Run all performance tests:
```bash
pytest tests/performance/ -m performance
```

### Run specific test categories:
```bash
# Throughput tests
pytest tests/performance/ -m throughput

# Latency tests
pytest tests/performance/ -m latency

# Memory profiling
pytest tests/performance/ -m memory

# Load tests
pytest tests/performance/ -m load

# Stress tests
pytest tests/performance/ -m stress
```

### Run with verbose output:
```bash
pytest tests/performance/ -v -m performance
```

## Performance Thresholds

### ETL Components
- **Extractors**: > 100 ops/sec
- **Loaders**: > 100 ops/sec
- **Transformers**: > 100 ops/sec
- **Pipeline**: < 20 seconds for 10K rows

### ML Components
- **Trainers**: < 2 minutes for 100K samples
- **Predictors**: > 1000 predictions/sec
- **Latency**: < 10ms average, < 20ms P95

### Memory
- **Transformers**: < 500MB increase for 100 iterations
- **Loaders**: < 1GB increase for large datasets
- **Trainers**: < 2GB increase for 100K samples

## Notes

- Performance tests may take longer to run than unit tests
- Some tests require significant memory
- Results may vary based on system resources
- Use `pytest-xdist` for parallel execution if needed


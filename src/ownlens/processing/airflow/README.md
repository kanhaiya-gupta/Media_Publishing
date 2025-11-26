# Airflow Orchestration Documentation

## Overview

This directory contains Apache Airflow DAGs and operators for orchestrating ETL and ML workflows in the OwnLens platform.

## Architecture Workflow

```mermaid
graph TB

    subgraph "📊 Data Sources"
        USER_EVENTS[User Events<br/>👤 Real-time]
        CONTENT_EVENTS[Content Events<br/>📰 Real-time]
        SESSION_EVENTS[Session Events<br/>🔄 Real-time]
        SECURITY_EVENTS[Security Events<br/>🔒 Real-time]
        COMPLIANCE_EVENTS[Compliance Events<br/>⚖️ Real-time]
        POSTGRES_SOURCE[(PostgreSQL<br/>🗄️ Operational Data)]
        MEDIA_FILES[Media Files<br/>📸 Batch/Stream]
    end

    subgraph "⚙️ Processing Layer"
        KAFKA[Kafka Streaming<br/>📡 Event Ingestion]
        SPARK_STREAMING[Spark Structured Streaming<br/>⚡ Real-time Processing]
        SPARK_BATCH[Spark Batch Processing<br/>🔄 ETL Transformations]
        AIRFLOW[Airflow Orchestration<br/>📅 Workflow Scheduling]
        DBT[DBT Transformations<br/>🔄 Data Modeling]
    end

    subgraph "💾 Data Storage"
        POSTGRES[(PostgreSQL<br/>🗄️ Operational DB)]
        CLICKHOUSE[(ClickHouse<br/>📊 OLAP Data Warehouse)]
        DELTA_LAKE[Delta Lake on MinIO/S3<br/>📦 Data Lake]
        MINIO[(MinIO/S3<br/>☁️ Object Storage)]
    end

    subgraph "☁️ Cloud Analytics"
        DATABRICKS[Databricks<br/>🔷 Cloud Analytics]
        SNOWFLAKE[(Snowflake<br/>❄️ Cloud Data Warehouse)]
    end

    subgraph "🤖 ML & Analytics"
        ML_TRAINING[ML Model Training<br/>🧠 Churn, Segmentation, Recommendations]
        ML_INFERENCE[ML Inference<br/>🔮 Real-time Predictions]
        REAL_TIME_ANALYTICS[Real-time Analytics<br/>📊 User Behavior]
        BUSINESS_INTELLIGENCE[Business Intelligence<br/>📈 Dashboards & Reports]
        EDITORIAL_ANALYTICS[Editorial Analytics<br/>✍️ Content Performance]
    end

    %% Data Flow - Sources to Processing
    USER_EVENTS --> KAFKA
    CONTENT_EVENTS --> KAFKA
    SESSION_EVENTS --> KAFKA
    SECURITY_EVENTS --> KAFKA
    COMPLIANCE_EVENTS --> KAFKA
    MEDIA_FILES --> MINIO
    
    KAFKA --> SPARK_STREAMING
    POSTGRES_SOURCE --> SPARK_BATCH
    MINIO --> SPARK_BATCH
    
    %% Processing to Storage
    SPARK_STREAMING --> DELTA_LAKE
    SPARK_STREAMING --> CLICKHOUSE
    SPARK_BATCH --> POSTGRES
    SPARK_BATCH --> CLICKHOUSE
    SPARK_BATCH --> DELTA_LAKE
    SPARK_BATCH --> MINIO
    
    %% Airflow Orchestration
    AIRFLOW --> SPARK_BATCH
    AIRFLOW --> ML_TRAINING
    DBT --> CLICKHOUSE
    
    %% Storage to Cloud
    DELTA_LAKE --> DATABRICKS
    CLICKHOUSE --> SNOWFLAKE
    
    %% Storage to ML & Analytics
    CLICKHOUSE -->|ML Features| ML_TRAINING
    CLICKHOUSE -->|Real-time Queries| ML_INFERENCE
    CLICKHOUSE -->|Analytics Queries| REAL_TIME_ANALYTICS
    CLICKHOUSE -->|Aggregated Data| BUSINESS_INTELLIGENCE
    CLICKHOUSE -->|Content Metrics| EDITORIAL_ANALYTICS
    
    POSTGRES -->|Operational Queries| REAL_TIME_ANALYTICS
    POSTGRES -->|User Data| BUSINESS_INTELLIGENCE
    
    DATABRICKS -->|Advanced Analytics| BUSINESS_INTELLIGENCE
    SNOWFLAKE -->|Cloud Analytics| BUSINESS_INTELLIGENCE
    
    ML_TRAINING --> ML_INFERENCE
    
    %% Styling
    classDef dataSource fill:#e1f5ff,stroke:#01579b,stroke-width:2px,color:#000
    classDef processing fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000
    classDef storage fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000
    classDef cloud fill:#e0f2f1,stroke:#004d40,stroke-width:2px,color:#000
    classDef mlAnalytics fill:#fce4ec,stroke:#880e4f,stroke-width:2px,color:#000

    class USER_EVENTS,CONTENT_EVENTS,SESSION_EVENTS,SECURITY_EVENTS,COMPLIANCE_EVENTS,POSTGRES_SOURCE,MEDIA_FILES dataSource
    class KAFKA,SPARK_STREAMING,SPARK_BATCH,AIRFLOW,DBT processing
    class POSTGRES,CLICKHOUSE,DELTA_LAKE,MINIO storage
    class DATABRICKS,SNOWFLAKE cloud
    class ML_TRAINING,ML_INFERENCE,REAL_TIME_ANALYTICS,BUSINESS_INTELLIGENCE,EDITORIAL_ANALYTICS mlAnalytics
```

**Key Data Flows:**

1. **Real-time Streaming**: User/Content/Session events → Kafka → Spark Streaming → Delta Lake/ClickHouse → Real-time Analytics
2. **Batch ETL**: PostgreSQL/S3 → Spark Batch → ClickHouse/PostgreSQL → Business Intelligence
3. **ML Pipeline**: ClickHouse → ML Training → Model Registry → ML Inference → Real-time Predictions
4. **Cloud Analytics**: Delta Lake → Databricks / ClickHouse → Snowflake → Advanced Analytics
5. **Orchestration**: Airflow schedules and coordinates all workflows (ETL, ML training, data quality checks)

## Directory Structure

```
src/ownlens/processing/airflow/
├── base/                    # Base classes for DAGs
│   ├── __init__.py
│   └── base_dag.py          # BaseETLDAG class
├── dags/                    # Airflow DAG definitions
│   ├── __init__.py
│   ├── etl_pipeline_dag.py      # ETL pipeline DAG
│   ├── ml_workflow_dag.py       # ML workflow DAG
│   └── master_pipeline_dag.py   # Master pipeline DAG
├── operators/               # Custom Airflow operators
│   ├── __init__.py
│   └── python_callable_operator.py  # PythonCallableOperator
├── utils/                   # Utility functions
│   ├── __init__.py
│   └── config.py            # AirflowConfig class
└── README.md               # This file
```

## Prerequisites

1. **Docker and Docker Compose** - Airflow runs in Docker containers
2. **PostgreSQL** - Airflow metadata database
3. **Redis** - Celery broker for distributed task execution

## Initial Setup

### 1. Generate Fernet Key

**IMPORTANT:** Airflow requires a Fernet key for encrypting sensitive data (passwords, connections, etc.). You must generate a unique key before starting Airflow.

**📖 For detailed Fernet key setup instructions, see [FERNET_KEY_SETUP.md](./FERNET_KEY_SETUP.md)**

**Quick Setup:**
```bash
# Generate a Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Example output:**
```
G4Asdn0xGOJOpUU-vK65pWC-y6BR81cM2lWRjXuk24E=
```

### 2. Update docker-compose.yml

Add the generated Fernet key to all Airflow services in `docker-compose.yml`:

```yaml
environment:
  - AIRFLOW__CORE__FERNET_KEY=YOUR_GENERATED_KEY_HERE
```

**Important Notes:**
- ⚠️ **Never commit the Fernet key to version control** - Use environment variables or secrets management
- ⚠️ **Keep the same key across all Airflow services** (webserver, scheduler, worker)
- ⚠️ **If you change the key, you'll need to re-encrypt all existing connections and variables**
- 📖 **See [FERNET_KEY_SETUP.md](./FERNET_KEY_SETUP.md) for detailed setup and troubleshooting**

### 3. Initialize Airflow Database

After starting the containers, initialize the Airflow database:

```bash
# Initialize the database
docker exec airflow-webserver airflow db init

# Or use the newer command (Airflow 2.8+)
docker exec airflow-webserver airflow db migrate
```

### 4. Create Admin User

Create an admin user for the Airflow web UI:

```bash
docker exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@ownlens.com \
  --password admin
```

**Note:** The docker-compose.yml already includes `_AIRFLOW_WWW_USER_CREATE=true` which should create the user automatically, but you can use this command if needed.

## Starting Airflow

### Start All Services

```bash
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
```

### Check Service Status

```bash
# Check if containers are running
docker ps --filter "name=airflow"

# Check logs
docker logs airflow-webserver
docker logs airflow-scheduler
docker logs airflow-worker
```

### Access Airflow UI

1. Open your browser: `http://localhost:8080`
2. Login with:
   - **Username:** `admin`
   - **Password:** `admin`

## DAGs Overview

### 1. ETL Pipeline DAG (`etl_pipeline`)

**Schedule:** Daily at midnight (`@daily`)

**Description:** Extracts data from PostgreSQL, Kafka, and S3, transforms it, and loads to S3, PostgreSQL, and ClickHouse.

**Tasks:**
- `run_etl_pipeline` - Runs the complete ETL pipeline

**Configuration:**
- Loads to all destinations: S3, PostgreSQL, ClickHouse
- Automatically sorts tables by dependency order for FK-safe loading

### 2. ML Workflow DAG (`ml_workflow`)

**Schedule:** Daily at 2 AM (`0 2 * * *`)

**Description:** Trains ML models, makes predictions, and monitors performance for customer and editorial domains.

**Tasks:**
- `ml_workflow_customer` - ML workflow for customer domain
- `ml_workflow_editorial` - ML workflow for editorial domain

**Note:** Both tasks run in parallel.

### 3. Master Pipeline DAG (`master_pipeline`)

**Schedule:** Daily at midnight (`@daily`)

**Description:** Orchestrates the complete data pipeline: ETL → ML workflows.

**Task Flow:**
```
start → etl_pipeline → [ml_workflow_customer, ml_workflow_editorial] → end
```

**Execution Timeout:** 6 hours (complete pipeline may take longer)

## Custom Operators

### PythonCallableOperator

Executes Python callable functions directly in Airflow tasks.

**Features:**
- Automatically cleans up Spark sessions after task completion
- Passes Airflow context variables (execution_date, data_interval) to functions
- Handles errors gracefully

**Usage:**
```python
from ..operators.python_callable_operator import PythonCallableOperator
from src.ownlens.processing.etl.orchestration import run_etl_pipeline

task = PythonCallableOperator(
    task_id="run_etl",
    python_callable=run_etl_pipeline,
    op_kwargs={
        "load": True,
        "load_destinations": ["s3", "postgresql", "clickhouse"]
    },
    dag=dag
)
```

## Configuration

### AirflowConfig

Configuration is managed through the `AirflowConfig` class in `utils/config.py`.

**Environment Variables:**
- `AIRFLOW_OWNER` - Default DAG owner (default: "ownlens")
- `AIRFLOW_EMAIL` - Default email for notifications (default: "admin@ownlens.com")
- `AIRFLOW_RETRIES` - Default number of retries (default: 3)
- `AIRFLOW_RETRY_DELAY` - Default retry delay in minutes (default: 5)
- `AIRFLOW_EXECUTION_TIMEOUT` - Default execution timeout in hours (default: 2)
- `SPARK_MASTER` - Spark master URL (default: "local[*]")

### BaseETLDAG

All DAGs inherit from `BaseETLDAG` which provides:
- Default retry configuration
- Email notifications on failure
- Common task dependencies
- Consistent DAG structure

## Testing DAGs

### List All DAGs

```bash
docker exec airflow-scheduler airflow dags list
```

### Validate DAG Syntax

```bash
# Validate a specific DAG
docker exec airflow-scheduler airflow dags test etl_pipeline 2025-11-10

# List DAGs with errors
docker exec airflow-scheduler airflow dags list-import-errors
```

### Trigger DAG Manually

```bash
# Trigger a DAG run
docker exec airflow-scheduler airflow dags trigger etl_pipeline

# Trigger with specific execution date
docker exec airflow-scheduler airflow dags trigger etl_pipeline --exec-date 2025-11-10
```

### Check DAG Status

```bash
# Show DAG details
docker exec airflow-scheduler airflow dags show etl_pipeline

# Show task instances
docker exec airflow-scheduler airflow tasks list etl_pipeline
```

## Troubleshooting

### Issue: Scheduler Container Exits

**Error:** `ERROR: You need to initialize the database`

**Solution:**
1. Ensure Fernet key is set in docker-compose.yml
2. Initialize the database:
   ```bash
   docker exec airflow-webserver airflow db init
   ```
3. Restart the scheduler:
   ```bash
   docker-compose restart airflow-scheduler
   ```

### Issue: Fernet Key Error

**Error:** `ValueError: Fernet key must be 32 url-safe base64-encoded bytes`

**Solution:**
1. Generate a new Fernet key:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
2. Update docker-compose.yml with the new key
3. Restart all Airflow containers

### Issue: DAGs Not Appearing

**Possible Causes:**
1. **DAG syntax errors** - Check logs:
   ```bash
   docker logs airflow-scheduler | grep ERROR
   ```
2. **Import errors** - Check DAG imports:
   ```bash
   docker exec airflow-scheduler airflow dags list-import-errors
   ```
3. **DAGs paused** - Unpause in UI or via CLI:
   ```bash
   docker exec airflow-scheduler airflow dags unpause etl_pipeline
   ```

### Issue: Docker Mount Errors

**Error:** `error while creating mount source path '/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/...': file exists`

**This is a known Docker Desktop on WSL issue that affects Airflow users on Windows since Docker Desktop 4.15+.**

**✅ PROVEN FIX (Works for 99% of cases):**

#### Step 1: Stop Everything & Clean Up

```powershell
# Open PowerShell or CMD as Administrator
cd C:\Users\kanha\Independent_Research\Media_Publishing

# Stop and remove ALL Airflow containers + volumes
docker-compose down -v --remove-orphans

# Stop any remaining containers
docker ps -q | ForEach-Object { docker stop $_ }

# Quit Docker Desktop completely (CRUCIAL!)
# Right-click the Docker whale in the system tray → Quit Docker Desktop
# Wait until NO Docker process is running:
tasklist | findstr docker
# → should return nothing
```

#### Step 2: Manually Delete the Corrupted Bind-Mount Folder

```bash
# Open WSL (Ubuntu-22.04)
wsl

# Delete the entire broken bind-mount cache
sudo rm -rf /run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/

# Optional but recommended: also clear the data cache
sudo rm -rf /run/desktop/mnt/host/wsl/docker-desktop-data/

# Exit WSL
exit
```

**Note:** Docker Desktop recreates these folders automatically on every start. Deleting them is 100% safe.

#### Step 3: Restart Docker Desktop Cleanly

1. Start Docker Desktop again (double-click desktop icon or Start menu)
2. Wait until the whale turns green and shows "Docker Desktop is running" (30–60 seconds)
3. Verify settings:
   - Docker Desktop → Settings → Resources → WSL Integration
   - Make sure "Ubuntu-22.04" is enabled ✓

#### Step 4: Full WSL2 Reset (One-Time)

```powershell
# In PowerShell (normal user, no admin needed)
wsl --shutdown
# Wait 10 seconds
# Docker Desktop will automatically restart the WSL2 backend
```

#### Step 5: Bring Airflow Back Up

```bash
cd /mnt/c/Users/kanha/Independent_Research/Media_Publishing

docker-compose up -d --force-recreate

# Check status
docker ps
```

#### Step 6: Final Verification

```bash
# 1. Open Airflow UI
start http://localhost:8080

# 2. Default login: admin / admin

# 3. Test CLI
docker exec -it airflow-scheduler airflow dags list

# 4. Check logs (should no longer be empty)
docker logs airflow-webserver
docker logs airflow-scheduler
```

**Alternative Solutions:**

1. **Use Single Container Approach** (recommended for development):
   - Switch to `LocalExecutor` with a single Airflow container
   - See "Alternative: Single Container Setup" section below

2. **Use Named Volumes Instead of Bind Mounts**:
   - Replace bind mounts with named volumes in docker-compose.yml
   - This avoids WSL mount path issues

**Prevention - Avoid This Happening Again:**

- ⚠️ **Never change the Fernet key while containers are running** → Always `docker-compose down -v` first
- ✅ Use named volumes instead of bind mounts for dags/plugins (optional but bulletproof)
- ✅ Docker Desktop → Settings → General → Uncheck "Use the WSL 2 based engine" → Apply → Re-enable it (forces full reset)

### Issue: Spark Sessions Not Closing

**Solution:** The `PythonCallableOperator` automatically cleans up Spark sessions after task completion. If you see Spark sessions not closing:

1. Check the operator's `finally` block is executing
2. Verify `SparkSessionManager.stop_session()` is being called
3. Check Airflow task logs for cleanup messages

## Best Practices

### 1. Fernet Key Management

- ✅ **DO:** Store Fernet key in environment variables or secrets management
- ✅ **DO:** Use the same key across all Airflow services
- ❌ **DON'T:** Commit Fernet key to version control
- ❌ **DON'T:** Change the key after initial setup (requires re-encryption)

### 2. DAG Development

- ✅ **DO:** Test DAGs locally before deploying
- ✅ **DO:** Use `BaseETLDAG` for consistent structure
- ✅ **DO:** Add descriptive tags to DAGs
- ❌ **DON'T:** Use hardcoded values - use configuration

### 3. Resource Management

- ✅ **DO:** Set appropriate execution timeouts
- ✅ **DO:** Use retries for transient failures
- ✅ **DO:** Clean up resources (Spark sessions, connections)
- ❌ **DON'T:** Run long-running tasks without timeouts

### 4. Monitoring

- ✅ **DO:** Monitor DAG runs in Airflow UI
- ✅ **DO:** Set up email notifications for failures
- ✅ **DO:** Check logs regularly
- ❌ **DON'T:** Ignore repeated failures

## Environment Variables Reference

### Required in docker-compose.yml

```yaml
environment:
  # Database connection
  - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:password@postgres:5432/ownlens
  
  # Celery configuration
  - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
  - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://postgres:password@postgres:5432/ownlens
  
  # Fernet key (REQUIRED - generate new key)
  - AIRFLOW__CORE__FERNET_KEY=YOUR_GENERATED_KEY_HERE
  
  # DAG configuration
  - AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
  - AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins
  - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
  - AIRFLOW__CORE__LOAD_EXAMPLES=false
```

## Quick Start Checklist

- [ ] Generate Fernet key
- [ ] Update docker-compose.yml with Fernet key
- [ ] Start Airflow containers: `docker-compose up -d airflow-webserver airflow-scheduler airflow-worker`
- [ ] Initialize database: `docker exec airflow-webserver airflow db init`
- [ ] Verify containers are running: `docker ps --filter "name=airflow"`
- [ ] Access Airflow UI: `http://localhost:8080`
- [ ] Verify DAGs are visible: `docker exec airflow-scheduler airflow dags list`
- [ ] Test a DAG: `docker exec airflow-scheduler airflow dags test etl_pipeline 2025-11-10`

## Quick Reference: Docker Mount Fix

If you encounter Docker mount errors, see **[DOCKER_MOUNT_FIX.md](./DOCKER_MOUNT_FIX.md)** for the proven fix.

**Quick fix sequence:**
```powershell
docker-compose down -v --remove-orphans
# Quit Docker Desktop from system tray
wsl
sudo rm -rf /run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/
exit
# Start Docker Desktop
wsl --shutdown
# Wait 10 seconds
docker-compose up -d --force-recreate
```

## Alternative: Single Container Setup

If you're experiencing Docker mount issues with the multi-container setup, you can use a simpler single-container approach:

### Benefits:
- ✅ Avoids Docker mount issues on WSL
- ✅ Simpler setup (no Redis/Celery needed)
- ✅ Easier to debug
- ✅ Good for development

### Configuration:

Use `LocalExecutor` with a single container that runs both webserver and scheduler:

```yaml
airflow:
  image: apache/airflow:2.8.0
  container_name: airflow
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:password@postgres:5432/ownlens
    - AIRFLOW__CORE__FERNET_KEY=YOUR_GENERATED_KEY_HERE
    - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
    - AIRFLOW__CORE__LOAD_EXAMPLES=false
  volumes:
    - ./src/ownlens/processing/airflow/dags:/opt/airflow/dags
    - ./src/ownlens/processing/airflow/plugins:/opt/airflow/plugins
    - airflow-logs:/opt/airflow/logs
  ports:
    - "8080:8080"
  depends_on:
    postgres:
      condition: service_healthy
  command: >
    bash -c "
      airflow db init &&
      airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@ownlens.com --password admin &&
      airflow webserver --port 8080 &
      airflow scheduler &
      wait
    "
```

**Note:** This approach is simpler but less scalable than CeleryExecutor. Use CeleryExecutor for production.

## Verifying DAGs and Troubleshooting

### Check DAG Import Errors

If DAGs are not appearing in the Airflow UI, check for import errors:

```bash
docker exec airflow-scheduler airflow dags list-import-errors
```

**Expected output when everything is working:**
```
No data found
```

**If there are errors**, you'll see detailed tracebacks showing which files have issues. Common issues:
- Missing Python dependencies (install them in the Dockerfile)
- Syntax errors in Python files
- Import path issues (check PYTHONPATH in docker-compose.yml)

### List All DAGs

Verify that your DAGs are loaded correctly:

```bash
docker exec airflow-scheduler airflow dags list
```

**Expected output:**
```
dag_id          | filepath               | owner   | paused
================+========================+=========+=======
etl_pipeline    | etl_pipeline_dag.py    | ownlens | True
master_pipeline | master_pipeline_dag.py | ownlens | True
ml_workflow     | ml_workflow_dag.py     | ownlens | True
```

### Check Container Status

Verify all Airflow containers are running:

```bash
docker ps --filter "name=airflow" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**Expected output:**
```
NAMES               STATUS                   PORTS
airflow-worker      Up X minutes             8080/tcp
airflow-scheduler   Up X minutes             8080/tcp
airflow-webserver   Up X minutes (healthy)  0.0.0.0:8080->8080/tcp
```

### View Container Logs

Check logs for errors:

```bash
# Scheduler logs
docker logs airflow-scheduler --tail 50

# Webserver logs
docker logs airflow-webserver --tail 50

# Worker logs
docker logs airflow-worker --tail 50
```

### Enable Automatic DAG Execution

**By default, DAGs are paused when created.** To enable automatic execution, unpause them:

```bash
# Unpause individual DAGs
docker exec airflow-scheduler airflow dags unpause etl_pipeline
docker exec airflow-scheduler airflow dags unpause ml_workflow
docker exec airflow-scheduler airflow dags unpause master_pipeline

# Or unpause all DAGs at once
docker exec airflow-scheduler airflow dags unpause --all
```

**DAG Schedules:**
- `etl_pipeline`: Runs daily at midnight (`@daily`)
- `ml_workflow`: Runs daily at 2 AM (`0 2 * * *`)
- `master_pipeline`: Runs daily at midnight (`@daily`)

Once unpaused, DAGs will run automatically according to their schedule. You can also unpause DAGs from the Airflow UI by toggling the pause/unpause switch next to each DAG.

### Disable Automatic DAG Execution

To disable automatic execution and pause DAGs:

```bash
# Pause individual DAGs
docker exec airflow-scheduler airflow dags pause etl_pipeline
docker exec airflow-scheduler airflow dags pause ml_workflow
docker exec airflow-scheduler airflow dags pause master_pipeline

# Or pause all DAGs at once
docker exec airflow-scheduler airflow dags pause --all
```

When paused, DAGs will not run automatically according to their schedule. You can still trigger them manually. You can also pause DAGs from the Airflow UI by toggling the pause/unpause switch next to each DAG.

### Test DAG Execution

Test a DAG manually (unpause it first in the UI or via CLI):

```bash
# Unpause a DAG
docker exec airflow-scheduler airflow dags unpause etl_pipeline

# Trigger a DAG run manually (even if paused)
docker exec airflow-scheduler airflow dags trigger etl_pipeline

# Check DAG run status
docker exec airflow-scheduler airflow dags state etl_pipeline <run_id>

# List recent DAG runs
docker exec airflow-scheduler airflow dags list-runs -d etl_pipeline --state running
```

### Common Issues and Solutions

1. **DAGs not appearing in UI:**
   - Check import errors: `docker exec airflow-scheduler airflow dags list-import-errors`
   - Verify DAGs are in the correct directory: `docker exec airflow-scheduler ls -la /opt/airflow/dags`
   - Check scheduler logs: `docker logs airflow-scheduler`

2. **Import errors:**
   - Missing dependencies: Add them to `Dockerfile.airflow` and rebuild
   - Syntax errors: Fix Python syntax issues in source files
   - Import path issues: Verify PYTHONPATH includes `/opt/airflow/src`

3. **Containers not starting:**
   - Check Docker mount errors (see Docker Mount Fix section above)
   - Verify Fernet key is set correctly
   - Check container logs for specific errors

4. **Tasks failing:**
   - Check task logs in Airflow UI
   - Verify dependencies are installed in the container
   - Check that source code is properly mounted

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Airflow Executors Comparison](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html)

## Support

For issues or questions:
1. Check the [Verifying DAGs and Troubleshooting](#verifying-dags-and-troubleshooting) section above
2. Review the [Docker Mount Fix](#docker-mount-fix) section for WSL2 mount issues
3. Check the [Fernet Key Setup](./FERNET_KEY_SETUP.md) guide for encryption key issues
4. Review Airflow logs: `docker logs airflow-scheduler` (or `docker logs airflow` for single container)
5. Check DAG import errors: `docker exec airflow-scheduler airflow dags list-import-errors`

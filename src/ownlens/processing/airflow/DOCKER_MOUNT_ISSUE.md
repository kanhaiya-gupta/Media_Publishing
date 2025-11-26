# Docker Mount Issue - Question for Grok

## Problem Statement

I'm experiencing a Docker mount error when trying to start Airflow containers (webserver and scheduler) on Windows with WSL2. The containers are stuck in "Created" status and cannot start.

## Error Details

**Error Message:**
```
Error response from daemon: error while creating mount source path '/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/Ubuntu-22.04/5910b7f60a00f8de89af44b21b8d948ecf49eeb0a1460f857547988a6d6fa5a4': mkdir /run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/Ubuntu-22.04/5910b7f60a00f8de89af44b21b8d948ecf49eeb0a1460f857547988a6d6fa5a4: file exists
```

**Container Status:**
- `airflow-webserver`: Status "Created" (not running)
- `airflow-scheduler`: Status "Created" (not running)
- `airflow-worker`: Status "Up 2 days" (running fine)

**Container Inspect Output:**
```bash
docker inspect airflow-webserver --format '{{.State.Status}} {{.State.Error}}'
# Output: created error while creating mount source path...
```

## Context

- **OS:** Windows 10/11 with WSL2 (Ubuntu 22.04)
- **Docker:** Docker Desktop for Windows
- **Setup:** Multiple Airflow containers (webserver, scheduler, worker) using CeleryExecutor
- **Volumes:** Bind mounts from WSL filesystem to containers:
  ```yaml
  volumes:
    - ./src/ownlens/processing/airflow/dags:/opt/airflow/dags
    - ./src/ownlens/processing/airflow/plugins:/opt/airflow/plugins
    - airflow-logs:/opt/airflow/logs
  ```

## What I've Tried

1. ✅ Removed and recreated containers: `docker rm -f airflow-webserver airflow-scheduler && docker-compose up -d`
2. ✅ Stopped and started containers: `docker-compose stop && docker-compose up -d`
3. ✅ Checked container logs: Empty (containers never started)
4. ✅ Verified Fernet key is set correctly in docker-compose.yml
5. ❌ **Restarting Docker Desktop** - Not yet tried (need guidance)

## Question for Grok

**How do I properly restart Docker Desktop on Windows to fix Docker mount path issues with WSL2?**

Specifically:
1. What's the correct procedure to restart Docker Desktop completely?
2. Should I stop all containers first, or can I restart Docker Desktop with containers running?
3. Are there any specific steps to ensure the mount paths are cleaned up?
4. After restarting, what should I check to verify the issue is resolved?
5. Are there any Docker Desktop settings I should check/change to prevent this issue?
6. Is there a way to clean up the problematic mount paths manually before restarting?

## Additional Information

- The `airflow-worker` container is running fine with the same volume mounts
- Only `airflow-webserver` and `airflow-scheduler` are affected
- This started happening after updating docker-compose.yml with a new Fernet key
- The error suggests Docker is trying to create a mount path that already exists

## Expected Outcome

After restarting Docker Desktop, I should be able to:
- Start `airflow-webserver` and `airflow-scheduler` containers successfully
- Access Airflow UI at `http://localhost:8080`
- Run `docker exec airflow-scheduler airflow dags list` without errors

## Alternative Solutions (if restart doesn't work)

1. Switch to single-container setup with LocalExecutor
2. Use named volumes instead of bind mounts
3. Check Docker Desktop WSL integration settings

---

**Please provide step-by-step instructions for restarting Docker Desktop and verifying the fix.**






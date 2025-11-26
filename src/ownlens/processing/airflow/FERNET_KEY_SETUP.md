# Fernet Key Setup Guide

## What is a Fernet Key?

The Fernet key is used by Airflow to encrypt sensitive data such as:
- Database connection passwords
- API keys
- Variables containing secrets
- Connection credentials

**⚠️ CRITICAL:** Without a valid Fernet key, Airflow cannot start or will fail when trying to encrypt/decrypt sensitive data.

## Quick Setup

### Step 1: Generate Fernet Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Example output:**
```
G4Asdn0xGOJOpUU-vK65pWC-y6BR81cM2lWRjXuk24E=
```

### Step 2: Update docker-compose.yml

Find all occurrences of `AIRFLOW__CORE__FERNET_KEY` in `docker-compose.yml` and replace with your generated key:

```yaml
environment:
  - AIRFLOW__CORE__FERNET_KEY=G4Asdn0xGOJOpUU-vK65pWC-y6BR81cM2lWRjXuk24E=
```

**Important:** Update this in **ALL** Airflow services:
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-worker`

### Step 3: Restart Containers

```bash
# Stop containers
docker-compose stop airflow-webserver airflow-scheduler airflow-worker

# Remove containers (if needed)
docker rm airflow-webserver airflow-scheduler airflow-worker

# Start containers
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
```

### Step 4: Initialize Database

```bash
docker exec airflow-webserver airflow db init
```

## Common Errors

### Error: "Fernet key must be 32 url-safe base64-encoded bytes"

**Cause:** Empty or invalid Fernet key

**Solution:**
1. Generate a new key using the command above
2. Update docker-compose.yml
3. Restart containers

### Error: "Could not create Fernet object"

**Cause:** Invalid Fernet key format

**Solution:**
1. Ensure the key is exactly 44 characters (including the `=` at the end)
2. Ensure there are no quotes around the key in docker-compose.yml
3. Ensure the key is on a single line

### Error: "Database initialization fails"

**Cause:** Fernet key not set before database initialization

**Solution:**
1. Set Fernet key in docker-compose.yml
2. Restart containers
3. Initialize database again

## Security Best Practices

### ⚠️ DO NOT:

- ❌ Commit Fernet key to version control
- ❌ Share Fernet key publicly
- ❌ Use the same key across different environments (dev/staging/prod)
- ❌ Change the key after data has been encrypted (requires re-encryption)

### ✅ DO:

- ✅ Store Fernet key in environment variables or secrets management
- ✅ Use different keys for different environments
- ✅ Keep the key secure and backed up
- ✅ Use the same key across all Airflow services in the same environment

## Using Environment Variables (Recommended)

Instead of hardcoding the Fernet key in docker-compose.yml, use environment variables:

### 1. Create `.env` file (DO NOT commit to git)

```bash
# .env (add to .gitignore)
AIRFLOW_FERNET_KEY=G4Asdn0xGOJOpUU-vK65pWC-y6BR81cM2lWRjXuk24E=
```

### 2. Update docker-compose.yml

```yaml
environment:
  - AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY}
```

### 3. Load environment variable

```bash
# Load from .env file
export $(cat .env | xargs)

# Or set directly
export AIRFLOW_FERNET_KEY=G4Asdn0xGOJOpUU-vK65pWC-y6BR81cM2lWRjXuk24E=

# Start containers
docker-compose up -d
```

## Verification

### Check if Fernet key is set correctly:

```bash
# Check webserver
docker exec airflow-webserver env | grep FERNET_KEY

# Check scheduler
docker exec airflow-scheduler env | grep FERNET_KEY

# Check worker
docker exec airflow-worker env | grep FERNET_KEY
```

All should show the same key value.

### Test database initialization:

```bash
docker exec airflow-webserver airflow db init
```

If successful, you should see:
```
DB: postgresql+psycopg2://postgres:***@postgres:5432/ownlens
[INFO] Context impl PostgresqlImpl.
[INFO] Will assume transactional DDL.
```

## Troubleshooting

### Issue: Key works in webserver but not scheduler

**Solution:** Ensure all services have the same key. Check docker-compose.yml for all Airflow services.

### Issue: Key changed but existing connections fail

**Solution:** If you change the Fernet key after data has been encrypted:
1. Export all connections/variables from Airflow UI
2. Update the key
3. Re-import connections/variables (they will be re-encrypted)

### Issue: Key is correct but still getting errors

**Solution:**
1. Verify key format (44 characters, ends with `=`)
2. Check for extra spaces or quotes
3. Ensure key is on a single line
4. Restart all containers

## Quick Reference

```bash
# Generate key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Check key in container
docker exec airflow-webserver env | grep FERNET_KEY

# Initialize database
docker exec airflow-webserver airflow db init

# Restart services
docker-compose restart airflow-webserver airflow-scheduler airflow-worker
```

## Additional Resources

- [Airflow Fernet Key Documentation](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html)
- [Cryptography Fernet Documentation](https://cryptography.io/en/latest/fernet/)






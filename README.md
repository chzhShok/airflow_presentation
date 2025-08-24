# What is it?
Presentation, docker compose and DAG examples for Airflow

# Presentation
Located in `docs/`  
Presentation `Airflow`: more basic information  
Presentation `Airflow_performance_edition`: less basic information, more specific information

# Start Airflow in Docker
Airflow version: 3.0.3

### Clone repository
```bash
git clone git@github.com:chzhShok/airflow_presentation.git
cd airflow_presentation/docker
```

### Set `GITHUB_FERNET_KEY_AIRFLOW` variable
Generate fernet key
```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

Set variable
MacOs/Linux
```bash
echo 'export GITHUB_FERNET_KEY_AIRFLOW="your-fernet-key-here"' >> ~/.zshrc # or ~/.bashrc
source ~/.zshrc # or ~/.bashrc
```

### Start Docker containers
For new Docker versions
```bash
docker compose up -d
```

For old Docker versions
```bash
docker-compose up -d
```

# About this Docker Compose

This build features:
1. Uses a remote Git repository and a local `dags` directory as DAG bundles. See `[dag_processor]` in `airflow.cfg`
2. Creates a table `test_table` in the `postgres` container using the `init.sql` script at startup
3. Includes the `airflow-init-connection` container, which creates connections in the Airflow metadata database at startup

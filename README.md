# TakeHome_Akalamio

This project contains a set of Python scripts and SQL queries designed to perform database operations and analyze user interaction data. It is organized to facilitate database connections, perform operations, and execute specific data modeling tasks within a containerized environment.

## Project Structure

```
TakeHome_Akalamio/
├── conn_utils.py                # Utilities for managing database connections
├── db_operations.py             # Scripts for performing database operations
├── scenario_1.py                # Script for executing a specific scenario
├── data_model_scripts/          # Directory containing SQL queries
│   ├── clickThroughRate.sql     # SQL for calculating click-through rate
│   ├── referralPageLoad.sql     # SQL for referral page load data
│   ├── referralRecommendClick.sql # SQL for referral recommendation clicks
│   ├── uniqueUserClicks.sql     # SQL for unique user click data
```

## Containerized Environment

### Prerequisites

- Docker installed on your machine
- Docker Compose installed for multi-service orchestration

### Setting Up the Environment

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd TakeHome_Akalamio
   ```

2. Ensure `docker-compose.yml` includes the following services:
   - **Airflow**: For orchestration
   - **PostgreSQL**: As the database

#### Sample `docker-compose.yml`
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"

  airflow:
    image: apache/airflow:2.6.1
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    depends_on:
      - postgres
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
```

3. Start the services:
   ```bash
   docker-compose up -d
   ```

4. Access Airflow UI:
   - URL: [http://localhost:8080](http://localhost:8080)
   - Default credentials: `airflow` / `airflow`

### Usage

#### 1. Database Connection
The `conn_utils.py` script provides utility functions for connecting to the PostgreSQL database. Ensure you configure the connection parameters in the script to match your database setup.

#### 2. Running Scenarios
- The `scenario_1.py` script demonstrates how to execute operations for a specific scenario.
- To run the script inside the Airflow container:
  ```bash
  docker exec -it <container_name> python scenario_1.py
  ```

#### 3. Database Operations
- The `db_operations.py` script includes utility functions for various database operations, such as executing queries and fetching results.

#### 4. SQL Queries
- SQL scripts for data modeling are located in the `data_model_scripts/` directory.
- Each script is designed to perform a specific task, such as calculating click-through rates or analyzing unique user clicks.

### Customization
- Update `conn_utils.py` to configure database credentials and connection settings.
- Modify SQL scripts in `data_model_scripts/` to suit your specific requirements.

## Example Workflow
1. Start the containerized environment using Docker Compose.
2. Configure the database connection in `conn_utils.py`.
3. Run the Python scripts (`scenario_1.py` or `db_operations.py`) to perform database operations.
4. Use the SQL scripts in `data_model_scripts/` for detailed data modeling and analysis.

## License
This project is licensed under the MIT License.

## Author
**Shahid Hassan Nasir**

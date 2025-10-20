# Port Management System

An integrated port management solution that orchestrates container operations between ships and trucks using Camunda Zeebe workflow engine.

## Features

- Container loading/unloading operations
- Crane operation management
- Weighing station integration
- Storage management
- Truck check-in/check-out processing
- Real-time workflow orchestration

## Prerequisites

- Python 3.7+
- PostgreSQL database
- Camunda Platform 8 (Zeebe)
- Required Python packages (see requirements.txt)

## Configuration

Database settings in `config/database.py`:
```python
DB_CONFIG = {
    "dbname": "portmanagement",
    "user": "postgres",
    "password": "srinija",
    "host": "localhost",
    "port": 5432
}
```

Zeebe connection:
```python
ZEEBE_ADDRESS = "localhost:26500"
```

## Usage

1. Start in Worker Mode (process tasks):
```bash
python flow.py
# Select option 1
```

2. Start in Workflow Mode (initiate workflows):
```bash
python flow.py
# Select option 2
```

3. Run both Worker and Workflow modes:
```bash
python flow.py
# Select option 3
```

## Database Schema

- process_instance: Tracks workflow instances
- container: Container information
- storage: Storage management
- transport_mean: Ship and truck details

## Operation Types

- Loading
- Unloading
- Weighing
- Storage
- Truck Check-in
- Truck Check-out

## Monitoring

Access Camunda Operate at: http://localhost:8080/operate

## File Structure

```
├── config/
│   └── database.py
├── handlers/
│   ├── crane_operations.py
│   ├── storage_operations.py
│   ├── truck_operations.py
│   └── weighing_operations.py
├── database/
│   ├── schema.sql
│   └── sample_data.sql
├── flow.py
└── README.md
```
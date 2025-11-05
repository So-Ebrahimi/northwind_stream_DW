# Data-pipeline-for-northwind

## Non-local run (CI)

If you prefer not to run anything locally, trigger the GitHub Actions workflow:

- Go to Actions → "Debezium CDC for Northwind" → Run workflow.
- The workflow brings up Kafka, Postgres, and Debezium in CI and registers the Northwind Postgres CDC connector automatically.

Details and local config are under `debezium/`, `kafka/`, and `postgres/` if needed.

Data Quality Monitoring with Polars
Project Overview

This is a learning project for me to completely explore the polars libray.

This project aims to build a robust and efficient Data Quality (DQ) monitoring and alerting system leveraging the high-performance capabilities of the Polars data manipulation library. The primary goal is to ensure the integrity and reliability of data by automatically detecting common quality issues (e.g., missing values, inconsistencies, invalid formats) and triggering alerts when predefined thresholds are breached.

Phase 1: Core Data Quality Checks

    [ ] Define and acquire a sample dataset and place it in data/raw/.
    [ ] Implement a basic Polars data loading script (data_quality_checks.py) to read data from data/raw/.
    [ ] Develop the first data quality check: Completeness (Null Value Percentage) per column using Polars expressions.
    [ ] Implement the second data quality check: Uniqueness (Duplicate Count) for key columns (e.g., ID fields).
    [ ] Implement a basic Validity Check (e.g., is_in for categorical values, simple regex for patterns like email, or date format validation).
    [ ] Create config.yaml to define columns to check and initial thresholds for each quality metric.
    [ ] Refactor DQ checks into reusable functions within data_quality_checks.py.

Phase 2: Alerting & Reporting

    [ ] Integrate a logging mechanism to record check results (success/failure, metrics).
    [ ] Implement an alerting function (e.g., send an email via smtplib or post to a Slack/Teams webhook) when a check fails its threshold.
    [ ] Update config.yaml with alerting configurations (e.g., email recipients, webhook URLs, alert thresholds).
    [ ] Add a basic reporting mechanism (e.g., print a summary of all checks at the end of execution).

Phase 3: Orchestration & Deployment

    [ ] Create a Dockerfile to containerize the Polars DQ script.
    [ ] Build and test the Docker image locally.
    [ ] Select an orchestration tool (e.g., Mage, Airflow, Dagster).
    [ ] Develop a basic pipeline/DAG definition for the chosen orchestrator to run data_quality_checks.py periodically.
    [ ] (Optional) Explore deploying the Dockerized solution to a cloud service (e.g., AWS Fargate, Google Cloud Run).

Phase 4: Enhancements & Refinements

    [ ] Add more sophisticated data quality checks (e.g., cross-column consistency, range checks, anomaly detection).
    [ ] Implement historical tracking of data quality metrics (e.g., storing results in a small database like DuckDB).
    [ ] Develop unit tests for the data quality check functions.
    [ ] Improve error handling and resilience of the script.


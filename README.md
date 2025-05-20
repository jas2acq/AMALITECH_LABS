# Flight Price Analysis Pipeline

This repository contains the Flight Price Analysis Pipeline, an Apache Airflow-based ETL workflow for processing flight price data from Bangladesh. The pipeline ingests, validates, transforms, computes KPIs, and loads data into a PostgreSQL warehouse.

## Getting Started
For detailed setup instructions and an overview of the project, please refer to the following documents:

- **[Overview](Overview.md)**: A high-level summary of the pipeline, including its architecture and execution flow.
- **[Setup](Setup.md)**: Instructions for installing dependencies, configuring the Docker environment, and understanding the project structure.

## Detailed Documentation
For a comprehensive report covering the following, refer to [Flight_Price_Analysis_Pipeline_Report.md](Flight_Price_Analysis_Pipeline_Report.md):
- **Pipeline Architecture and Execution Flow**: Overview of the data flow and task sequence.
- **Description of Each Airflow DAG/Task**: Detailed breakdown of the `Main_pipeline` DAG and its tasks.
- **KPI Definitions and Computation Logic**: Explanation of computed KPIs (e.g., average fare per airline).
- **Challenges Encountered and How They Were Resolved**: Issues faced (e.g., long-running tasks) and their solutions.

## Next Steps
- Clone the repository and follow the setup instructions.
- Run `docker-compose up -d` to start the services.
- Access the Airflow UI at `http://localhost:8080` to monitor the `Main_pipeline` DAG.
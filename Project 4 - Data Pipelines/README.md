Data Pipelines with Airflow
---

This project goal is to create high-grade data pipelines using Apache Airflow for a music streaming 
company called Sparkify. The company wants to introduce more automation and monitoring to their data
warehouse ETL pipelines, and they have decided that Apache Airflow is the best tool for this.

The project requires the creation of dynamic and reusable tasks, which can be monitored and allow easy
backfills. The company has emphasized that data quality plays a significant part in analyses executed
on top of the data warehouse. Therefore, tests need to be run against their datasets after the ETL 
steps have been executed to catch any discrepancies.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon 
Redshift. The source datasets consist of JSON logs that tell about user activity in the application
and JSON metadata about the songs the users listen to.

## Project Overview

The project will involve the following tasks:

1. Create custom operators to perform tasks such as staging the data, filling the data warehouse,
and running checks on the data.
2. Create a DAG to orchestrate the tasks and allow for easy backfills.
3. Test the data quality by running tests against the datasets in the data warehouse.
4. Monitor the data pipelines using Airflow's built-in tools.

## Project Structure

The project repository has the following structure:
- **dag**: This file contains the DAG definition.
- **operators**: This folder contains the custom operators.
- **README.md**: This file provides an overview of the project and describes how to execute the code.

## Getting Started

### Prerequisites
- AWS account (S3, IAM, and Redshift access)
- Python 3.x
- Apache Airflow 2.x

### Execution
- Set up the AWS credentials and connection details in Airflow's web UI.
- Run the DAG in Airflow's web UI to execute the ETL pipeline.

## Acknowledgments
- Udacity for providing the project requirements and template.
- Apache Airflow documentation for reference.
- Stack Overflow and other online communities for troubleshooting help.
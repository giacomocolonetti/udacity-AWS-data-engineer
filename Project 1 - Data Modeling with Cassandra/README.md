Data Modeling with Apache Cassandra
---
This project goal is to create a database using Apache Cassandra to analyze song play data for
a startup called Sparkify. Currently, the data resides in a directory of CSV files on user 
activity on the app.

The analysis team at Sparkify is interested in understanding what songs users are listening to and
would like a data engineer to create an Apache Cassandra database which can create queries to answer
the questions. The data engineer's role is to create a database for this analysis and test the 
database by running queries given by the analytics team from Sparkify to create the results.

## Project Overview

In this project, we will apply what we've learned on data modeling with Apache Cassandra and complete
an ETL pipeline using Python. To complete the project, we will need to model the data by creating
tables in Apache Cassandra to run queries. We are provided with part of the ETL pipeline that transfers
data from a set of CSV files within a directory to create a streamlined CSV file to model and insert
data into Apache Cassandra tables.

## Project Structure

- **etl.ipynb**: This Jupyter Notebook walks through the ETL process and provides the necessary steps to 
extract, transform and load the data into the Apache Cassandra tables.
- **event_data**: This folder contains the source data in CSV format.
- **README.md**: This file provides an overview of the project and describes how to execute the code.

## Prerequisites
- Python 3.x
- Apache Cassandra 3.x

## Execution
Follow the instructions in the etl.ipynb notebook to extract, transform and load the data into the 
Apache Cassandra tables.

## Acknowledgments

- Udacity for providing the project requirements and template.
- Apache Cassandra documentation for reference.
- Stack Overflow and other online communities for troubleshooting help.
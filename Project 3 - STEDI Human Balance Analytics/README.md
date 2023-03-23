STEDI Human Balance Analytics
---

This project aims to build a lakehouse solution in AWS using AWS Glue, AWS S3, Python, and Spark.
The goal is to satisfy the requirements from the STEDI data scientists.

## Project Overview

The goal of this project is to process data from a step tracker while preserving the anonymity of users.
The processed data is then used to provide the data science team with a curated dataset to train machine
learning models.

To simulate the data coming from the various sources, we've created S3 directories for 
customer_landing, step_trainer_landing, and accelerometer_landing zones and used example data.
Two Glue tables are created for the two landing zones, customer_landing and accelerometer_landing.

Two AWS Glue Jobs are created to sanitize the data from the Website (Landing Zone) and only store
the Customer Records who agreed to share their data for research purposes (Trusted Zone), creating
a Glue Table called customer_trusted, and to sanitize the Accelerometer data from the Mobile App
(Landing Zone) and only store Accelerometer Readings from customers who agreed to share their data
for research purposes (Trusted Zone), creating a Glue Table called accelerometer_trusted.

A Glue job is written to sanitize the Customer data (Trusted Zone) and create a Glue Table 
(Curated Zone) that only includes customers who have accelerometer data and have agreed to share
their data for research called customers_curated. Finally, two Glue Studio jobs are created to 
read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called 
step_trainer_trusted that contains the Step Trainer Records data for customers who have 
accelerometer data and have agreed to share their data for research (customers_curated) 
and create an aggregated table that has each of the Step Trainer Readings, and the associated 
accelerometer reading data for the same timestamp, but only for customers who have agreed to
share their data, and make a glue table called machine_learning_curated.
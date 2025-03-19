# EMR Restaurant Violations Analysis

This project provides tools to analyze restaurant health inspection violations data using Amazon EMR. It includes a PySpark job for data transformation and a Python client application for job submission and results retrieval.

# Project Overview
The application processes restaurant violation data to identify establishments with "RED" violations (critical health violations) and counts the total violations per restaurant.

# Components
*PySpark Job (main.py)*: Transforms CSV data to count and aggregate RED violations by restaurant

*Client Application (client.py)*: Submits jobs to EMR clusters and retrieves results

*AWS Infrastructure*: Utilizes S3 for storage and EMR for distributed processing

#  Requirements
Python 3.6+

AWS account with appropriate permissions

AWS CLI configured with credentials

# Python dependencies:
boto3

pandas

pyarrow (for reading parquet files)

tabulate (for displaying results)

# Setup Instructions

1, Install Required Packages

```bash
pip install boto3 pandas pyarrow tabulate
```

2, Upload Data and Code to S3

3, Create an EMR Cluster

# Client Arguments

--cluster-id: EMR cluster ID

--region: AWS region (default: us-east-2)

--script: S3 path to the PySpark script

--input: S3 path to the input data

--output: S3 path for the output data

--wait: Wait for job completion (optional)

--show-results: Show results after job completion (optional)

--limit: Maximum number of result rows to display (default: 10)

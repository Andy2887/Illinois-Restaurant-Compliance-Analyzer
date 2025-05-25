# Illinois Restaurant Compliance Analyzer

A comprehensive big data processing pipeline designed to identify and analyze restaurant health code violations across Illinois using Amazon EMR and Apache Spark. This project processes large-scale restaurant inspection data to identify critical health violations and provide insights into compliance patterns.

## Project Overview

The Illinois Restaurant Compliance Analyzer is a scalable data processing solution that:

- **Processes large-scale restaurant inspection data** from Illinois health departments
- **Identifies critical "RED" violations** that pose immediate health risks to consumers
- **Aggregates violation counts** by restaurant to identify repeat offenders
- **Provides actionable insights** for health inspectors and public health officials
- **Leverages cloud computing** for efficient processing of massive datasets

### Key Features

- **Distributed Processing**: Uses Apache Spark on Amazon EMR for scalable data processing
- **Automated Analysis**: Processes CSV data files and generates structured parquet outputs
- **Cloud Integration**: Seamless integration with AWS S3 for data storage and retrieval
- **Real-time Monitoring**: Job status tracking and result visualization
- **Flexible Configuration**: Customizable parameters for different analysis scenarios

### Data Processing Pipeline

1. **Data Ingestion**: Restaurant inspection data is uploaded to S3 in CSV format
2. **Distributed Processing**: PySpark job processes data across EMR cluster nodes
3. **Violation Analysis**: Filters and aggregates critical "RED" violations by restaurant
4. **Results Storage**: Outputs processed data in efficient parquet format
5. **Visualization**: Client application retrieves and displays results in tabular format

## Components

### 1. PySpark Job (`main.py`)
- **Purpose**: Core data transformation engine
- **Functionality**: 
  - Reads CSV data from S3
  - Filters for "RED" violations (critical health code violations)
  - Aggregates violation counts by restaurant name
  - Outputs results in parquet format for efficient storage and querying
- **Technology**: Apache Spark SQL for distributed data processing

### 2. Client Application (`client.py`)
- **Purpose**: Job orchestration and result management
- **Functionality**:
  - Submits Spark jobs to EMR clusters
  - Monitors job execution status
  - Retrieves and displays processing results
  - Provides flexible command-line interface
- **Technology**: Python with boto3 for AWS integration

### 3. AWS Infrastructure
- **Amazon S3**: Scalable storage for input data and processing results
- **Amazon EMR**: Managed Hadoop/Spark cluster for distributed processing
- **IAM Roles**: Secure access management for AWS resources

## Prerequisites

### System Requirements
- **Python 3.6+** (Python 3.8+ recommended)
- **AWS Account** with appropriate permissions for EMR, S3, and IAM
- **AWS CLI** configured with valid credentials
- **Sufficient AWS quotas** for EMR cluster creation

### Required AWS Permissions
Your AWS user/role must have permissions for:
- EMR cluster creation and management
- S3 bucket read/write access
- IAM role assumption (for EMR service roles)
- CloudWatch logs access (for monitoring)

### Python Dependencies
Install the following packages:

```bash
# Core AWS integration
boto3>=1.26.0

# Data processing and display
pandas>=1.3.0
pyarrow>=5.0.0  # For parquet file support
tabulate>=0.9.0  # For formatted result display
```

## Setup Instructions

### 1. Environment Setup

#### Install Required Packages
```bash
# Install all dependencies at once
pip install boto3 pandas pyarrow tabulate

# Or install from requirements file (if available)
pip install -r requirements.txt
```

#### Configure AWS Credentials
```bash
# Configure AWS CLI with your credentials
aws configure

# Verify configuration
aws sts get-caller-identity
```

### 2. Prepare Your Data

#### Data Format Requirements
Your input CSV file must contain the following columns:
- `Name`: Restaurant name
- `Violation Type`: Type of violation (should include "RED" for critical violations)

#### Upload Data to S3
```bash
# Create S3 bucket (if not exists)
aws s3 mb s3://your-bucket-name

# Upload your restaurant inspection data
aws s3 cp your-data.csv s3://your-bucket-name/input/restaurant-data.csv

# Upload the PySpark script
aws s3 cp main.py s3://your-bucket-name/scripts/main.py
```

### 3. Create EMR Cluster

#### Option A: Using AWS Console
1. Navigate to EMR in AWS Console
2. Create cluster with Spark application
3. Note the cluster ID for use with the client

#### Option B: Using AWS CLI
```bash
# Create EMR cluster with Spark
aws emr create-cluster \
    --name "Restaurant-Compliance-Analyzer" \
    --release-label emr-6.15.0 \
    --applications Name=Spark \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --use-default-roles \
    --region us-east-2
```

### 4. Configure IAM Roles (if needed)
```bash
# Create default EMR roles if they don't exist
aws emr create-default-roles
```

## Usage

### Basic Usage

#### Submit and Monitor a Job
```bash
python3 client.py \
  --cluster-id j-YOUR-CLUSTER-ID \
  --region us-east-2 \
  --script s3://your-bucket/scripts/main.py \
  --input s3://your-bucket/input/restaurant-data.csv \
  --output s3://your-bucket/output/ \
  --wait \
  --show-results
```

#### Submit Job Without Waiting
```bash
python3 client.py \
  --cluster-id j-YOUR-CLUSTER-ID \
  --script s3://your-bucket/scripts/main.py \
  --input s3://your-bucket/input/restaurant-data.csv \
  --output s3://your-bucket/output/
```

### Advanced Usage Examples

#### Process Multiple Datasets
```bash
# Process Q1 data
python3 client.py \
  --cluster-id j-YOUR-CLUSTER-ID \
  --script s3://your-bucket/scripts/main.py \
  --input s3://your-bucket/input/q1-violations.csv \
  --output s3://your-bucket/output/q1/ \
  --wait

# Process Q2 data
python3 client.py \
  --cluster-id j-YOUR-CLUSTER-ID \
  --script s3://your-bucket/scripts/main.py \
  --input s3://your-bucket/input/q2-violations.csv \
  --output s3://your-bucket/output/q2/ \
  --wait
```

#### Show Limited Results
```bash
python3 client.py \
  --cluster-id j-YOUR-CLUSTER-ID \
  --script s3://your-bucket/scripts/main.py \
  --input s3://your-bucket/input/restaurant-data.csv \
  --output s3://your-bucket/output/ \
  --wait \
  --show-results \
  --limit 25
```

### Command-Line Arguments

#### Required Arguments
- `--cluster-id`: EMR cluster ID (e.g., j-1VE06SA9NF1LV)
- `--script`: S3 path to the PySpark script (main.py)
- `--input`: S3 path to the input CSV data file
- `--output`: S3 path for output data directory

#### Optional Arguments  
- `--region`: AWS region (default: us-east-2)
- `--wait`: Wait for job completion before exiting
- `--show-results`: Display results table after job completion
- `--limit`: Maximum number of result rows to display (default: 10)

## Output Format

The processed results are stored in parquet format with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `name` | String | Restaurant name |
| `total_red_violations` | Integer | Count of critical violations |

### Sample Output
```
Restaurant Violations Summary
┌─────────────────────────────────────┬─────────────────────────┐
│ Restaurant Name                     │ Total RED Violations    │
├─────────────────────────────────────┼─────────────────────────┤
│ Joe's Pizza & Grill                 │ 15                      │
│ Downtown Diner                      │ 12                      │
│ Main Street Cafe                    │ 8                       │
│ Corner Bakery                       │ 6                       │
└─────────────────────────────────────┴─────────────────────────┘
```

## Monitoring and Troubleshooting

### Check Job Status
```bash
# Check EMR cluster status
aws emr describe-cluster --cluster-id j-YOUR-CLUSTER-ID

# List steps for a cluster
aws emr list-steps --cluster-id j-YOUR-CLUSTER-ID
```

### Common Issues and Solutions

#### Issue: "Cluster not found"
- **Solution**: Verify cluster ID and ensure cluster is in WAITING state

#### Issue: "Access denied" errors
- **Solution**: Check IAM permissions for EMR, S3, and CloudWatch access

#### Issue: "No data in results"
- **Solution**: Verify input data contains "RED" violations in the "Violation Type" column

#### Issue: Job fails with memory errors
- **Solution**: Increase EMR cluster instance size or add more nodes

### Logs and Debugging
```bash
# View cluster logs
aws emr describe-cluster --cluster-id j-YOUR-CLUSTER-ID \
  --query 'Cluster.LogUri'

# View step logs in S3 console at the LogUri location
```

## Project Structure
```
Illinois-Restaurant-Compliance-Analyzer/
├── README.md              # This documentation
├── main.py               # PySpark data processing job
├── client.py             # EMR job submission client
├── command.txt           # Example commands and usage
└── requirements.txt      # Python dependencies (if created)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample data
5. Submit a pull request

## Acknowledgement

This project is part of CS310 coursework at Northwestern University.

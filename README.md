# Airline Data Ingestion Pipeline

This project implements an end-to-end airline data ingestion pipeline using various AWS services. The pipeline seamlessly integrates daily file uploads to Amazon S3, triggering CloudTrail notifications, and orchestrating the entire process using AWS EventBridge rules and Step Functions.

## Workflow Overview

1. **File Uploads to S3**: Daily airline data files are uploaded to an S3 bucket.

2. **CloudTrail Notifications**: CloudTrail monitors the S3 bucket for file uploads and triggers notifications upon file arrival.

3. **EventBridge Rules**: EventBridge rules are set up to intercept S3 upload events and initiate the Step Function workflow.

4. **Step 1: Schema Discovery with Glue Crawler**: The workflow begins by initiating a Glue crawler to fetch the schema from the uploaded files in the S3 bucket. This process automatically creates tables in the Glue Data Catalog.

5. **Step 2: Data Transformation and Ingestion with Glue Job**: A Glue job is executed to transform and ingest the data from S3 into Redshift tables, based on the discovered schema.

6. **Step 3: Notification via SNS**: Upon completion of the pipeline steps, success or failure notifications are sent via Amazon SNS, keeping subscribers informed about the status of the ingestion process.

## Components Used

- **Amazon S3**: Stores the daily airline data files.
- **AWS CloudTrail**: Monitors S3 bucket events and triggers notifications.
- **AWS Glue**: Utilized for schema discovery, data transformation, and ingestion.
- **Amazon Redshift**: Hosts the data warehouse for storing the ingested airline data.
- **AWS Step Functions**: Orchestrates the workflow for seamless data processing.
- **Amazon EventBridge**: Acts as the event bus, triggering the Step Function workflow.
- **Amazon SNS**: Sends notifications to subscribers regarding pipeline status updates.




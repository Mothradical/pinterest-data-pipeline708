# Pinterest Data Pipeline

## Aim of Project

The aim of this project was to design a data pipeline for batch and streaming data, utilising Apache Kafka, Amazon Web Services' cloud technology and Databricks.

Over the course of this project, I learned to see the big picture of how all these services are integrated. Though I knew plenty about these services as a fact, I didn't truly appreciate the intricacy until actually executing a project with them myself.

## Table of Contents

- [Installation and Instructions](#installation-and-instructions)
  - [Installation](#installation)
  - [Tools and Dependencies](#tools-and-dependencies)
- [File Structure of Project](#file-structure-of-project)
- [Building the Pipeline](#building-the-pipeline)
  - [Set up the project environment](#set-up-the-project-environment)
  - [Familiarise yourself with the data structure](#familiarise-yourself-with-the-data-structure)
  - [Configure EC2 Kafka Client](#configure-ec2-kafka-client)
  - [Connect MSK Cluster to S3 bucket](#connect-msk-cluster-to-s3-bucket)
  - [Configure API in API Gateway](#configure-api-in-api-gateway)
  - [Batch Data Processing in Databricks](#batch-data-processing-in-databricks)
  - [AWS MWAA](#aws-mwaa)
  - [AWS Kinesis](#aws-kinesis)

## Installation

### Dependencies

The following dependencies are needed to run this project:

- python-dotenv
- sqlalchemy
- requests

As this project was run using confidential details for access to the database, AWS, and Databricks, it cannot be run in its current form by others. It can, however, be adapted to work for a different project.

### Tools

- Apache Kafka - An event streaming platform

- Apache Spark - A multi-language engine for performing data processes on clusters, utilised in this project as a feature of Databricks.

- AWS API Gateway - Part of AWS, this service facilitates the creation and maintainance of APIs.

- AWK Kinesis - Part of AWS, this service processes streaming data. In this project, data were streamed to Kinesis before being read into Databricks for cleaning and analysis.

- Amazon Managed Streaming for Apache Kafka (AWS MSK) - Part of AWS, this service allows users to build and run applications that utilise Kafka to process streaming data.

- AWS MSK Connect - Feature of AWS MSK that utilises the Kafka Connect framework to enable the streaming of data between Apache Kafka clusters and external systems.

- Databricks - A cloud-based lakehouse platform utilised in this proect to perform Spark processing on both batch and streaming data.

- Kafka REST Proxy - Provides a RESTful interface to an Kafka cluster, allowing for administrative tasks to be performed on the cluster without the use of native Kafka protocol or clients.

- Managed Workflows for Apache Airflow (MWAA) - Apache Airflow allows 

## File Structure of Project

user_posting_emulation.py: This Python file emulates the processing of data to an S3 bucket by pulling random records from an online database. Database credentials are stored in a separate yaml file.

user_posting_emulation_streaming.py: This Python file emulates the processing of data to a Kinesis Data Stream by pulling random records from an online database and separating them with a PartitionKey. Database credentials are stored in a separate yaml file.

load_s3_to_databricks.py: This python file contains code used in Databricks to read from an AWS S3 bucket, then clean and query that data.

load_kinesis_to_databricks.py: This file is used to read streaming data from AWS Kinesis, then transform and clean that data before uploading it to Delta tables.

1254dc635ec5_dag.py: This Python file contains a DAG to be uploaded to a bucket in an AWS MWAA environment. This DAG is set to trigger a Databricks notebook based on load_s3_to_databricks.py daily, thus emulating batch processing.

README.md: This is the file you're reading right now, containing information about the project.

.gitignore: This file indicates the files containing credentials which could not be shared for security purposes.

## Building the Pipeline

As this project was conducted as part of a bootcamp, certain elements pre-exist my involvement. For example, I did not need to create a cluster. Thus, these instructions do not cover the creation of some elements.

### Set up the project environment

Create the conda virtual environment and install the required libraries.

### Familiarise yourself with the data structure

Remove the "#" from lines 66-68 of the file user_posting_emulation.py and run it to produce an infine output of randomly selected data from the tables. Keep in mind, as this script reads the database credentials from a locally stored yaml file, you will not be able to do this without your own credentials.

### Configure EC2 Kafka Client

As long as you have an AWS IAM user with the appropriate permissions, you should be able to do the following:
- Find the key-pair value associated with your IAM user and save it locally to a .pem file
- Launch an EC2 instance in the terminal following the instructions provided in the terminal.
- To set up Kafka in the EC2 instance, first install the IAM MSK authentication package on the client EC2 machine.
- Configure your IAM role security for cluster authentication by adding a principle to your trust policy. For me, this was by copying the arn of a role under the name of '<my_UserId>-ec2-access-role', adding a new principle, selecting 'IAM roles' and the Principle type, and replacing the arn with the one I'd copied.
- To configure the Kafka client to use AWS IAM authentication to the cluster, you must modify the clients.properties file in the kafka_folder/bin directory with the appropriate details.
- Find the Bootstrap servers string and Plaintext Apache Zookeeper connection string for your MSK cluster.
- Set your CLASSPATH environment to the location of aws-msk-iam-auth-1.1.5-all.jar in the libs directory.
- Using the CLI in the EC2 client, create your Kafka topics (for my project, it was <my_userID>.pin, <my_userID>.geo, and <my_userID>.user).
- In the create topic Kafka command replace the BootstrapServerString with the one for the MASK cluster obtained 3-steps prior.

### Connect MSK Cluster to S3 bucket

- Create an S3 bucket (one was already created for me).
- On the EC2 client, download the Confluent.io Amazon S3 Connector and copy it to your bucket.
- In the MSK Connect console, create a custom plugin designed to connect to the S3 bucket.

### Configure API in API Gateway

- An API had already been created for me.
- Create a Resource that allows you to build a PROXY integration for you API.
- Create a HTTP ANY method for your resource. When setting up the endpoint URL, made sure you use the Public DNS from the EC2 machine as part of it. e.g: http://<PublicDNS>:8082/{proxy}
- Deploy the API and make note of the Invoke URL
- To setup the proxy on the EC2 client machine, install the Confluent package for the proxy onto the client machine.
- Allow the REST proxy to perform IAM authentication to the MSK cluster by modifying the kafka-rest.properties file with 'nano kafka-rest.properties'. You will need your configure your access role arn, Boostrap server string, and Plaintext Apache Zookeeper connection string in this file.
- It's ready to go. Run user_posting_emulation.py (make sure # are present at the start of lines 66-68) and data will be pulled from the database and sent to the S3 bucket, organised into topics.

### Batch Data Processing in Databricks

- Set up a Databricks account
- My account was set-up to have the neccessary permissions to read from S3 directly without mounting the bucket. You may need to mount the bucket, for which you'll also need certain permissions.
- The file load_s3_to_databricks.py contains the code I wrote to read my S3 bucket into Databricks, creating three different dataframes (one for each topic), cleaning them, and running a series of queries. Each separate notebook command is indicated by '# COMMAND'.
- You will have to adjust to code to your specifications, such as replacing part of the path to the S3 topics with your IAM userID.

### AWS MWAA

- My AWS account was already provided with access to an MWAA environment and its bucket. You may not have this, but will need one to continue.
- The file 1254dc635ec5_dag.py contains the DAG I used, though you will need to edit this to your specifications. It is currently set to run daily, though you may wish to edit this.
- You must then upload your dag.py file to your bucket in the MWAA environment.
- Manually trigger it to check that it works.
- If user_posting_emulation.py were to run continuously, the whole process would now emulate batch data processing. New data are uploaded continuously, then analysed in intervals.

### AWS Kinesis

- With batch processing done, it's time for stream processing.
- My AWS account was granted permission to use PutRecord and describe an existing Kinesis stream. You will need these permissions to continue.
- You will need the arn of an IAM role with Kinesis access.
- Return to the API you created earlier and allow it to invoke Kinesis actions. You may need to create an IAM role for API access to Kinesis, though I was already provided with one. Cope its arn.
- You will need to create a new resource called "streams"  with a get method.then a resource under that called "{stream-name}" with DELETE, GET, and POST methods, then two resources under that called "record" and "records" each with a PUT method. Use the arn from the previous step for the Execution role when setting up these methods.
- Run user_posting_emulation_streaming.py to send data to Kinesis streams. Make adjustments to the script to suit your needs.
- Create a new notebook in Databricks based on the file load_kinesis_to_databricks. Once again, you will need your own permissions to access AWS.
- Make sure that data are being uploaded to Kinesis Data Streams. Run the notebook and the data will be read into Databricks from Kinesis as a Sparks dataframe with encoded data, which is then split into different dataframes based on the partition key (topics). The encoded data are then converted to JSON, then converted into columns and records, cleaned, and finally uploaded to Delta tables which are stored in Databricks. These are now ready for querying using SQL.
- As the Delta tables are appended with each new record received, if user_posting_emulation_streaming.py and the Databricks notebook were to run continuously, this would emulate streaming data processing.

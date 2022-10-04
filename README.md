# Import MySQL tables to Bigquery Project
Initiave that creates a basic ETL using GCP components such as Cloud Functions, Bigquery API and Cron Scheduler

<!-- ABOUT THE PROJECT -->
## About The Project

The intend of this project is to connect a MySQL host based DB and ingest to a GCP repository (BigQuery)

### Prerequisites
There are assumptions that must be applied before runs the code:
* GCP Account with API activated
* DB in MySQL with open connection
* Proper Python libraries installed
  * google-api-core
  * mysql-connector-python
  * mysqlclient
  * numpy
  * pandas
  * pyarrow

### Steps to ingest MySQL data to BigQuery
It was used the following steps:
* Created a SA with proper permission to generate BigQuery jobs
* Created a connection file `mysql_connection.py` which contains properties of the MySQL connection
* Created a connection file `bigquery_connection.py` which contains properties of the BigQuery connection
* Created a function per each table with proper schemas and add ingested timestamp
* Created a main script which runs the data pipeline
* For each zone, created a Google Cloud Function and enable triggering by url request
  * For fact tables in Landing Zone, we append 70 days data in their raw tables
  * For fact tables in Staging Zone, we get the last ingested data using SQL
  * For fact tables in Production Zone, we apply incremental load using dynamic SQL 
* Created a file `main.py` in Trigger Cloud Function folder for triggering the data pipeline
* Pass url Cloud Function into a Google Scheduler and run the script with the desired cadence

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement". Don't forget to give the project a star! Thanks again!

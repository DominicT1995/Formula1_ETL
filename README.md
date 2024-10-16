# Formula-1-ETL
![F1 Banner Image](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/f1_banner.jpg?raw=true)

## Project Goal:
The primary aim of this project is to create a functioning ETL pipeline for Formula 1 race data by utilizing PySpark and Spark SQL in Microsoft Azure Databricks. In addition, Azure Data Factory will be used to automate and schedule the ETL process for given race data from the Ergast API. 

schema applied, audit columns, stored in parquet, analyze using SQL, incremental load : join key info into new table : analysis and reporting of standings via dashboards : pipeline scheduling and monitoring

## Process:

#### Setup:

As apart of the initial setup for the ETL pipeline in Microsoft Azure, a Databricks workspace for this project was created along with a data lake with necessary storage containers for our files including:

* raw - used to store raw, uncleaned data
* processed - used to store cleaned raw data
* presentation - used to store finalized merged data tables used for analysis and dashboarding
* demo - used for accessing and performing transformations and manipulations on data tables before applying to the raw and processed data

In the Databricks workspace, a compute with necessary nodes and compute power was created and tied to the Azure account. The Azure Data Lake Storage containers were then mounted using a service principal. This was achieved by using dbutils capabilities to acquire necessary client id, tenant id, and client secret from the Azure account and then setting the appropriate spark configurations.

For this project, static csv and json data was used from the Ergast API which supplies F1 race data dating back to the first championship in 1950. Initially, all data ranging from 1950 to 2021 was uploaded at once to the raw container with SQL raw database and table creation. The purpose of this was to simulate a full load scenario before later transitioning to a more desirable incremental load architecture where cutover files and weekly race files were uploaded. For the purposes for creating the ETL pipeline architecure, only two sets of race data, 2021-03-28 and 2021-04-18, were incrementally loaded and merged into the cutover file of 2021-03-21. For incremental load setup, all files were directly added to the raw adls container.

#### Data Ingestion:

Creation of processed database for table saving, managed tables

Steps to ingest and clean all files

merging function of updated data for incremental load cutover vs weekly

#### Data Transformation:

joining tables and adding ranks and calculated points

create presentation database

#### Data Analysis:

sql table queries

dashboard creation

![Driver Standings](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/dashboard_drivers.png?raw=true)

![Constructor Standings](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/dashboard_teams.png?raw=true)

#### Pipeline Creation:

azure datafactory pipeline architecture

![Ingestion Pipeline](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/pl_ingestion.png?raw=true)

![Transformation Pipeline](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/pl_transformation.png?raw=true)

![Transformation Workflow](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/workflow_pl_transform.png?raw=true)

![Processing Pipeline](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/pl_process.png?raw=true)

trigger addition

scheduled trigger firing

![Trigger History]{https://github.com/DominicT1995/Formula1_ETL/blob/main/images/trigger_runs.png?raw=true}

future addition of code to automatically pull from api into raw container 
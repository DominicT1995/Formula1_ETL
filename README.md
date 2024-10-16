# Formula-1-ETL
![F1 Banner Image](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/f1_banner.jpg?raw=true)

## Project Goal:
The primary aim of this project is to create a functioning ETL pipeline for Formula 1 race data by utilizing PySpark and Spark SQL in Microsoft Azure Databricks. In addition, Azure Data Factory will be used to automate and schedule the ETL process for given race data from the Ergast API. 

## Process:

### Setup:

As apart of the initial setup for the ETL pipeline in Microsoft Azure, a Databricks workspace for this project was created along with a data lake with necessary storage containers for our files including:

* raw - used to store raw, uncleaned data
* processed - used to store cleaned raw data
* presentation - used to store finalized merged data tables used for analysis and dashboarding
* demo - used for accessing and performing transformations and manipulations on data tables before applying to the raw and processed data

In the Databricks workspace, a compute with necessary nodes and compute power was created and tied to the Azure account. The Azure Data Lake Storage containers were then mounted using a service principal. This was achieved by using dbutils capabilities to acquire necessary client id, tenant id, and client secret from the Azure account and then setting the appropriate spark configurations. A configuration notebook was then created to save all mount paths for raw, processed, and presentation containers for ease of access through the project.

For this project, static csv and json data was used from the Ergast API which supplies F1 race data dating back to the first championship in 1950. Initially, all data ranging from 1950 to 2021 was uploaded at once to the raw container with SQL raw database and table creation. The purpose of this was to simulate a full load scenario before later transitioning to a more desirable incremental load architecture where cutover files and weekly race files were uploaded. For the purposes for creating the ETL pipeline architecure, only two sets of race data, 2021-03-28 and 2021-04-18, were incrementally loaded and merged into the cutover file of 2021-03-21. For incremental load setup, all files were directly added to the raw adls container.

### Data Ingestion:

Table data taken from the Ergast API to be ingested includes the following:

* circuits
* races
* constructors (teams)
* drivers
* results
* pit_stops
* lap_times
* qualifying

A SQL database for storing the processed tables was then created and attached to the adls processed container.

A single notebook was created for each ingested file utilizing common functions, defined in the includes folder, to assist in processing the data. Created functinos were used primarily for adding audit columns such as ingestion date and for overwriting/updating partitioned data during incremental load with parquet files. In addition, a merge function was created once data was transitioned from parquet to delta format.

The first step in our ingestion process was to use the Ergast API table documentation (https://ergast.com/docs/f1db_user_guide.txt) to apply the appropriate schema for loaded in spark dataframes.

Next, data was cleaned by selecting only necessary columns, dropping unwanted columns, adding audit columns for ingestion date and file date, renaming columns to utilize snake casing for consistency, concatenation of any seperated columns such as first and last name, and finally writing the finalized dataframe in columnar delta format as saved tables to the processed database created that is attached to the adls processed container. These tables can then be analyzed using SQL functionality.
- The results, pit_stops, lap_times, and qualifying data all vary from race to race. Therefore, this data was partitioned on the column 'race_id'.
- These files also update week to week with new incoming data from each race and therefore under an incremental load architecture, utilize the delta merge function to update all matching existing data and insert all unmatching data.

### Data Transformation:

In a similar manner to the ingestion step, a SQL database for storing presentation tables was created and attached to the presentation container and dataframes were loaded in from processed delta formatted tables.

For this project we are concerned with determining dominant drivers and teams throughout the years of F1 racing. Therefore idenitified tables of interest to determine this include:

* circuits
* drivers
* constructors
* races
* results

The first step is then to join all of these tables, utilizing primary and foreign key relationships. Thus, the circuits table is first joined to the races table on race_id, and then all other tables are joined into the results table on race_id. Necessary columns are then selected from this race_results table and the table is written the presentation database using a similar merge function designed for incremental load as in the ingestion step.

The next step is to take produce both driver and team standings by loading in the created race_results table and aggregating sum of earned points through all races, counting number of first positions as wins, and then window ranking based on partitioned race years and ordering by total points and number of race wins in the event of a point tie. These finalized driver and team standings can then be saved and merged in as another table in the presentation database. However, points are not standardized across all years of racing as fewer points were awarded for wins in the early years of F1 compared to now. This problem is addressed by created a new table based on race_results.

Addressing the issue of standardized points, a new table called calculated_race_results was created. In this table spark SQL was utilized to create a temporary view where necessary tables were once again joined and then calculated points were determined based on position in the race and results were filtered based on only top 10 finishers in the race. Incremental load is applied and these results are merged into the table from the created temporary view which queries data updates.

### Data Analysis:

Now with our presentation database and container updated with standing and calculated results tables, analysis can be performed utilizing SQL queries on any of these tables. 

For the purposes of this project, dominant drivers and teams will be determined across the years of racing by using the calculated_results table to use a standardized scoring system. Sum and average aggregations are performed on points earned, grouped by drivers/teams and race years. Filters were also added to prevent drivers/teams with few well performing races from skewing the point averages calculated.

Databricks visualization function was then used to create visualizations of the resulting queries and to transform these visualizations into a neat dashboard for reporting on the data.

Driver standings:
![Driver Standings](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/dashboard_drivers.png?raw=true)

Team standings:
![Constructor Standings](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/dashboard_teams.png?raw=true)

### Pipeline Creation:

The final step of this project is to automate our data ingestion and transformation to keep the Formula 1 data updated and ready for analysis at anytime in the presentation database. This was achieved by utilizing Azure Data Factory to create multiple pipelines, workflows, and scheduled triggers.

First an ingestion pipeline was created with each of our individual ingestion notebooks that take raw data based on folder date and process it into the processed database. This was completed by creating the parameter p_window_end_date which is obtained by obtaining metadata from existing folders in the raw container. The parameter is set to activate as the current date which will align with the same date as when race data should be recieved. An if condition is then applied and if the folder is not present, the pipeline will not proceed any further. If the folder is present, then all ingestion notebooks will proceed to run.

Ingestion pipeline:
![Ingestion Pipeline](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/pl_ingestion.png?raw=true)

Next, a transformation pipeline was created in a similar manner using an if condition to once again confirm raw data is present before running transformation notebooks.

Tranformation pipeline:
![Transformation Pipeline](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/pl_transformation.png?raw=true)

The transformation workflow within the if condition works differently than the ingestion if the condition is met. Because driver standings and team standings are dependent on race results, these must occur after the race results notebook is run for updated standings in these tables. Calculated race results runs independently due to sourcing data directly from the processed database.

Transformation workflow:
![Transformation Workflow](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/workflow_pl_transform.png?raw=true)

The last pipeline created is the process pipeline which connects the ingestion and the transformation pipeline processes together. The transformation pipeline will now only run once the ingestion pipeline is activated and completes running the ingestion notebooks.

Final Processing pipeline:
![Processing Pipeline](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/pl_process.png?raw=true)

Lastly, to fully automate the ETL pipelines that have been created, a trigger is added and scheduled to run every Sunday at 10:00 P.M. which is the day of the week when races occur and new data is acquired. If there happens to not be a race, the get metadata function will halt the pipeline runs. The trigger and pipeline runs were then tested using the aforementioned file dates with all pipelines running successfully for the given past date file folders.

Trigger History:
![Trigger History](https://github.com/DominicT1995/Formula1_ETL/blob/main/images/trigger_runs.png?raw=true)

## Future Improvements:

In this project, static data is downloaded from the Ergast API and directly uploaded to the raw container folder in weekly/biweekly scheduling and only past data is utilized. For full automation, new code can be written to automatically pull new weekly race data from the API and upload it to the container to be ready for automated trigger runs to initiate the pipelines for newly acquired race data.

## Attribution:

This project was completed as apart of Rmesh Retnasamy's course on Azure Databricks & Spark for Data Engineers (PySpark/SQL) to enhance personal databricks skills in relation to creating functioning ETL pipelines for use in future personal projects.
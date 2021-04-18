# Postman Data Engineer Assignment

A private repo, to solve [this](https://docs.google.com/document/d/1RJjQHDxi7jOQVOq8lrkUFPVJO5vnozPRGO-bpzx13wM/edit) assignment

## Installation / Setup to start

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the packages mentioned in `requirements.txt` after cloning the project and performing `cd /path/to/the/clone/repo/`

```bash
pip3 install -r requirements.txt
```

Before running the script, ensure an SQL server is running at `localhost`, with username = `sa` and password = `Igobacca1@`. Also need to ensure pyodbc driver `ODBC Driver 17 for SQL Server` are installed in your system correctly.
I used 
```bash
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=Igobacca1@" -p 1433:1433 --name RBC  -d mcr.microsoft.com/mssql/server:latest
```
After ensuring DB is up and running, put the data you want to process, in `path/to/code_dir/data` folder, which will either be created automatically when the script is executed, or you can create it yourself.  
Then after placing the `csv` files in `data` folder, execute the script `main.py`

```bash
python3 main.py
```
This will process the `products.csv` file (should be present in `data` directory), and loads the data into SQL server, under the table name `data`.

## Tables and Schema
Table name = `data`

Schema = `([name], string), ([sku], string, primary_key), ([description], string), ([chg_dttm], datetime)`

The script creates the table if it already is not present, but anyhow, here is the DDL for the same
```SQL
CREATE TABLE master.dbo.[data] (
	name varchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	sku varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	description varchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	chg_dttm datetime NULL,
	CONSTRAINT PK__data__DDDF4BE6901E1F22 PRIMARY KEY (sku)
);
```

## What is done from “Points to achieve”
- [x] Your code should follow concept of OOPS
- [x] Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.<br>`The whole process gets completed in under 2 mins` <br>
  <br>
  ```bash
  Time elapsed for operation - 'Loading data into SQL, after retrieving it from CSV : products.csv' : 0:00:29.045618
  Time elapsed for operation - 'Loading data into SQL, after retrieving it from CSV : products2.csv' : 0:01:06.867120
  Time elapsed for operation - 'Loading data into SQL, after retrieving it from CSV : products3.csv' : 0:01:36.248423
  ```
  Where products.csv is the raw File, products2.csv is the same file again, products3.csv file contains some changesto validate if the update is happening and overall working logic of the code.
  Below code let us see the update we did in `products3.csv` is reflected or not in the table.
    ```SQL
    select * from data where name = 'Sajal Sirohi'
    ```
  Below is the result of the query : 

| name         | sku                | description                                                                        | chg_dttm           |
|--------------|--------------------|------------------------------------------------------------------------------------|--------------------|
| Sajal Sirohi | a-new-primary-key  | This is a good thing I think so.                                                   | 2021-04-16 19:35:01|
| Sajal Sirohi | lay-raise-best-end | Art community floor adult your single type. Per back community former stock thing. | 2021-04-16 19:35:01|
  

- [x] Support for updating existing products in the table based on `sku` as the primary key. (Yes, we know about the kind of data in the file. You need to find a workaround for it. <br>`For this approach I removed all the duplicates from SKU column to make it primary key` <br>
- [x] All product details are to be ingested into a single table
- [x] An aggregated table on above rows with `name` and `no. of products` as the columns. <br>
  `Instead of creating an Aggregated table, I chose to create an Aggregated View, which is much faster to create, and does not take up extra memory.`
  
```SQL_Latin1_General_CP1_CI_AS
select * from VW_DATA vd 
```
Result : 

| name             | no. of products |
|------------------|-----------------|
| Aaron Abbott     | 1               |
| Aaron Alexander  | 1               |
| Aaron Andrews    | 2               |
| Aaron Bailey     | 5               |
| Aaron Ayala      | 1               |
| Aaron Ballard    | 1               |
| Aaron Barton     | 1               |
| Aaron Bryant DDS | 1               |
| Aaron Bush       | 5               |

## Additional points / features

- [x] Airflow DAG to do a backup of the data by executing the script in the path `dags/scripts/backup.sql`. And uses `dags/data_backup_dag` to execute the SQL script.
- [x] Added an automated data ingestion pipeline, which periodically checks for new blobs upload, scheduled using `airflow` every 5 minutes, and downloads the blob which was uploaded after the last process ran, therefore ingesting data in real time and then triggers process to ingest the CSV file, and then move the processed file to `processed_file` folder.

## What I would have done if given more time
- Instead of adding only one cloud end point (azure-blob), add NoSQL, SQL or other RDBMS Cloud endpoint to the ingestion framework.
- Add an email notification system whenever the data ingestion happens and create a proper log file for the same, to track every process either in text format, or a metadata table, or either a pipe-delimited text file and then create an external table on it, to keep the logs data both in txt file and SQL flavor.
- Add proper restartability to track where the process went wrong, at which file it went wrong, and then start the process again from that point.
- Dockerize the whole application to run the pipeline as a container

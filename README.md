# Postman Data Engineer Assignment

A private repo, to solve [this](https://docs.google.com/document/d/1RJjQHDxi7jOQVOq8lrkUFPVJO5vnozPRGO-bpzx13wM/edit) assignment

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the packages mentioned in `requirements.txt` after cloning the project and performing `cd /path/to/the/clone/repo/`

```bash
pip3 install -r requirements.txt
```

## Usage
Before running the script, ensure an SQL server is running at `localhost`, with username = `sa` and password = `Igobacca1@`. Also need to ensure pyodbc driver `ODBC Driver 17 for SQL Server` are installed in your system correctly

```bash
python3 main.py
```
This will process the `products.csv` file (which should be present in the same folder as the project), and loads the data into SQL server, under the table name `data`.

## Tables and Schema
Table name = `data`

Schema = `([name], string), ([sku], string, primary_key), ([description], string)`

The script creates the table if it already is not present, but anyhow, here is the DDL for the same
```SQL
CREATE TABLE master.dbo.[data] (
	name varchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	sku varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	description varchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK__data__DDDF4BE6D0664E5E PRIMARY KEY (sku)
);
```

## What is done from “Points to achieve”
- [x] Your code should follow concept of OOPS
- [x] Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.<br>`The whole process gets completed in under 2 mins` <br>
  <br>
  ```bash
  Time elapsed for operation - 'Loading data into SQL, after retrieving it from CSV : products.csv' : 0:00:15.068280
  Time elapsed for operation - 'Loading data into SQL, after retrieving it from CSV : products2.csv' : 0:00:31.829158
  Time elapsed for operation - 'Loading data into SQL, after retrieving it from CSV : products3.csv' : 0:00:47.151718
  ```
  Where products.csv is the raw File, products2.csv is the same file again, products3.csv file contains some changesto validate if the update is happening and overall working logic of the code.
  Below code let us see the update we did in `products3.csv` is reflected or not in the table.
    ```SQL
    select * from data where name = 'Sajal Sirohi'
    ```
  Below is the result of the query : 

| name         | sku                | description                                                                        |
|--------------|--------------------|------------------------------------------------------------------------------------|
| Sajal Sirohi | a-new-primary-key  | This is a good thing I think so.                                                   |
| Sajal Sirohi | lay-raise-best-end | Art community floor adult your single type. Per back community former stock thing. |
  

- [x] Support for updating existing products in the table based on `sku` as the primary key. (Yes, we know about the kind of data in the file. You need to find a workaround for it. <br>`For this approach I removed all the duplicates from SKU column to make it primary key` <br>
- [x] All product details are to be ingested into a single table
- [x] An aggregated table on above rows with `name` and `no. of products` as the columns

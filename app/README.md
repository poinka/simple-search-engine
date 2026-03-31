## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the text documents required to index. Here you can find a sample of 1000 documents from `b.parquet` file from the original source.

### mapreduce
This folder stores the mapper `mapper1.py` and reducer `reducer1.py` scripts for the MapReduce pipelines.

### app.py
This is a Python file to write code to store index data in Cassandra. It loads the index data from HDFS and writes it to Cassandra/ScyllaDB tables. This file is called in `store_index.sh` script.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS.

### index.sh
A script to run the MapReduce pipelines and the programs to store data in Cassandra/ScyllaDB.

### prepare_data.py
The script that will create documents from parquet file. You can run it in the driver.

### prepare_data.sh
The script that will run the prevoious Python file and will copy the data to HDFS.

### build_input_data.py
A Python file to prepare the input data for the MapReduce pipelines. This file will read the documents from HDFS, process them and write the output back to HDFS in a format suitable for the MapReduce jobs.

### query.py
A Python file to write PySpark app that will process a user's query and retrieves a list of top 10 relevant documents ranked using BM25.

### requirements-indexer.txt
This file contains all Python depenedencies that are needed for the indexing part of the project.

### requirements-query.txt
This file contains all Python depenedencies that are needed for the querying part of the project.

### requirements.txt
This file contains all Python depenedencies that are needed for the project.

### search.sh
This script will be responsible for running the `query.py` PySpark app on Hadoop YARN cluster.

### start-services.sh
This script will initiate the services required to run Hadoop components. This script is called in `app.sh` file.

### store_index.sh
This script will create Cassandra/ScyllaDB tables and load the index data from HDFS to them using the `store_index.py` script.

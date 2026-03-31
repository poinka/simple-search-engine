# big-data-assignment2

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose
## Step 2: Clone the repository
```bash
git clone https://github.com/poinka/big-data-assignment2.git
cd big-data-assignment2
```
## Step 3: Download the dataset
Download the parquet file from [here](https://www.kaggle.com/datasets/jjinho/wikipedia-20230701?select=b.parquet) and place it in the `app` folder. The file should end with `.parquet`.
## Step 4: Run the command
```bash
docker compose up 
```
This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.
## Step 5: Run the search script
```bash
docker exec -it cluster-master bash
/app/search.sh 'your query here'
```
This will run the `query.py` PySpark app on Hadoop YARN cluster and will return the list of top 10 relevant documents ranked using BM25 for the given query. 

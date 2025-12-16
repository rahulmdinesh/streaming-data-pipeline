# Streaming Data using Apache Airflow, Kafka and Spark

This project constructs an ETL Pipeline that is orchestrated using Apache Airflow, to ingest data from the [Random User API](https://randomuser.me/api/), stream the data to an Apache Kafka topic which holds the data until it is consumed by an Apache Spark job that writes the data to a Cassandra table. Everything is containerized using Docker for ease of deployment and scalability.


## Getting Started

To get started with the project, follow these steps:

1. Clone the repository:

```bash 
git clone https://github.com/rahulmdinesh/streaming-data-pipeline.git
cd streaming-data-pipeline
```

2. Create a virtual environment

```bash 
python -m venv .venv  
```

3. Activate the virtual environment

```bash 
source .venv/bin/activate  
```

4. Install the dependencies
```bash
pip install -r requirements.txt
```

5. Start the containers using Docker Compose
```bash
docker-compose up -d
```

6. Launch the Airflow Web UI to trigger DAG runs
```bash
open http://localhost:8080/
```

7. Launh the Kafka Web UI to monitor Kafka cluster and topic metrics
```bash
open http://localhost:9021/
```

8. Once the DAG run successfully completes, run the python script to consume data from the Kafka topic and store the data in a Cassandra table
```bash
python spark-stream.py
```

9. In another terminal in the same directory, run the following command to open an interactive cqlsh session inside the Cassandra container
```bash
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
```

10. Use the following CQL queries to query data from the Cassandra table
```bash
select * from spark_stream.users;
select count(*) from spark_stream.users;
```

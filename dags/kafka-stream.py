from datetime import datetime
import uuid
from airflow.decorators import dag, task

DEFAULT_START_DATE = datetime(2025, 1, 1)

default_args = {
    "owner": "Rahul M Dinesh",
    "start_date": DEFAULT_START_DATE,
}

def get_data_from_api():
    import json, requests
    raw_data = requests.get("https://randomuser.me/api/")
    raw_data = raw_data.json()['results'][0]
    return raw_data
    
def format_raw_data(raw_data):
    data = {}
    location = raw_data['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = raw_data['name']['first']
    data['last_name'] = raw_data['name']['last']
    data['gender'] = raw_data['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                    f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = raw_data['email']
    data['username'] = raw_data['login']['username']
    data['dob'] = raw_data['dob']['date']
    data['registered_date'] = raw_data['registered']['date']
    data['phone'] = raw_data['phone']
    data['picture'] = raw_data['picture']['medium']

    return data

@dag(
    dag_id="data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["data_engineering", "etl", "pipeline"],
)
def data_pipeline():
    @task(task_id="stream_data_to_kafka")
    def stream_data_to_kafka():
        from kafka import KafkaProducer
        import json, time, logging

        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        curr_time = time.time()

        while True:
            if time.time() > curr_time + 60: #1 minute
                break
            try:
                raw_data = get_data_from_api()
                data = format_raw_data(raw_data)

                # Send the formatted data to the kafka topic named 'users'
                producer.send('users', json.dumps(data).encode('utf-8'))
            except Exception as e:
                logging.error(f'An error occured: {e}')
                continue
            
    stream_data_to_kafka()  
data_pipeline()



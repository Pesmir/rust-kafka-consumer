# rust-kafka-consumer

This is just a demo with some GenericObjects to see what would be possible with a rust consumer with an redpanda backend

## How to start
#### 1. Clone Repo
#### 2. Start docker container with redpanda
```sh
docker-compose up
```

#### 3. Start python producer
```sh
virtualenv venv 
source venv/bin/activate
which pip3 # check that it is pointing to venv
pip3 install aiokafka -U
python src/producer.py
```

#### 4. Start Kakfa consumer in rust

```sh
RUST_LOG="info,rdkafka::client=warn" cargo run
```

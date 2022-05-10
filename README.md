# rust-kafka-consumer

This is just a demo with some GenericObjects to see what would be possible with a rust consumer with an redpanda backend

## How to start
#### 1. Clone Repo
#### Start docker container with redpanda
```sh
docker-compose up
```
#### Start Kakfa consumer in rust
```sh
RUST_LOG="info,rdkafka::client=warn" cargo run
```

#### Start python producer
```sh
virtualenv venv 
source venv/bin/activate
which pip3 # check that it is pointing to venv
pip3 install aiokafka -U
python src/producer.py
```

# rust-kafka-consumer

This is just a demo with some GenericObjects to see what would be 
possible with a rust consumer.

## How to start
#### 1. Clone Repo
#### 2. Start docker container with redpanda
```sh
docker-compose --file docker/docker-compose.kafkasn.yml up
```

#### 3. Start python producer
```sh
python3.10 -m venv venv
source venv/bin/activate
pip install -U pip setuptools .
python src/producer.py
```

#### 4. Start Kakfa consumer in rust

```sh
RUST_LOG="info,rdkafka::client=warn" cargo run
```
It could take some time until the kafka actually sends data.

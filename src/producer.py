# pip install aiokafka -U

import aiokafka
import hashlib
import asyncio


async def produce_message(topic: str, message: str):
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        await producer.send_and_wait(topic, message.encode())
    finally:
        await producer.stop()


async def main():
    # Schedule three calls *concurrently*:
    for i in range(0, 1_000):
        animal_hash = hashlib.sha1(str(i).encode()).hexdigest()
        animal_prefix = f"Animal: {animal_hash}"
        data = f"{animal_prefix} -> " + "HUGEDATA"*100_000
        await produce_message("double-agent-1", data)
        await produce_message("double-agent-2", data)
        print(f"Produced {animal_prefix}")
    print("DONE")


if __name__ == "__main__":
    asyncio.run(main())

# pip install aiokafka -U

import aiokafka
import random
import asyncio
import json


class GenericObject(object):
    _default_values = (
        {}
    )  # k-> v() mapping for missing values during deserialization
    _null_values = {}  # k->v() mapping for None values during deserialization
    _valid_attributes = ["_data_dict"]

    def __init__(self, data=None):
        if data is None:
            data = {}
        self._data_dict = self._deserialize(data)

    def __getattribute__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError as ae:
            data_dict = self._data_dict
            if name not in data_dict:
                raise
            return data_dict[name]

    def __setattr__(self, name, value):
        if name in self.__class__._valid_attributes:
            return super().__setattr__(name, value)
        self._data_dict[name] = value

    def __eq__(self, other):
        return self._data_dict == other._data_dict

    def serialize(self):
        return self._data_dict

    def _deserialize(self, data_dict):
        for k, v in self._default_values.items():
            if k not in data_dict:
                data_dict[k] = v()
        for k, v in self._null_values.items():
            if k in data_dict and data_dict[k] is None:
                data_dict[k] = v()

        return data_dict

    def __hash__(self):
        try:
            return hash(json.dumps(self._data_dict, sort_keys=True))
        except:
            return hash(str(self._data_dict))

    def __repr__(self):
        return f"{type(self)} {self._data_dict}, Hash: {self.__hash__()}"

    def is_none(self):
        return self._data_dict is None

    @classmethod
    def create(cls, **kwargs):
        return cls(kwargs)

    @classmethod
    def from_data(cls, data, **kwargs):
        return cls(data)

    @classmethod
    def create_key(cls, id=None, second_id=None):
        return cls.create(id=id, second_id=second_id)

    def clone(self, **kwargs):
        cp = copy.deepcopy(self._data_dict)
        return self.create(**cp)

    def __deepcopy__(self, memo):
        return self.clone()


async def produce_message(topic: str, message: GenericObject):
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        # A smart guy decided to serialize GenericObjects like that! <3 
        await producer.send_and_wait(topic, json.dumps(message.serialize()).encode("utf-8"))
    finally:
        await producer.stop()


async def main():
    for _ in range(0, 100_000):
        # This could be some time series data
        val = GenericObject.create(
            metric="a_importent_metric",
            value={"data":[dict(ts=j, val=random.randint(0, 100)) for j in range(6)]},
        )
        await produce_message("double-agent-1", val)
        await produce_message("double-agent-2", val)
        print(f"Produced {val}")
    print("DONE")


if __name__ == "__main__":
    asyncio.run(main())

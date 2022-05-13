import pandas as pd
import timeit
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

class Algo1(object):

    """Algo substracts one of the metric"""
    def _calculation_inner(self, frame: pd.DataFrame):
        """Adds one to the whole dataframe
        :returns: dataframe

        """
        frame = frame - 1
        return frame
    
    def rust_wrapper(self, go_str: str):
        # Raw Layer - Grep the raw data that came from the processing
        raw_data: dict = json.loads(go_str)
        go = GenericObject.from_data(raw_data)

        result = self.calcuation(go)

        # Convert back to raw data for processing
        return json.dumps(result.serialize()).encode("utf-8")
    
    def calcuation(self, go: GenericObject):
        metric_name = go.metric
        # DataFrame Layer - Here we work with Datascience types
        df = pd.DataFrame.from_records(go.value["data"]).set_index("ts", drop=True)
        df = self._calculation_inner(df)

        # Convert back for GenericObject layer
        processed_data = df.reset_index().to_json(orient="records")
        processed_data = json.loads(processed_data)
        raw_go = {"metric": metric_name, "value": {"data":processed_data}}
        return GenericObject.from_data(raw_go)



algo1_rust_version = Algo1()

if __name__ == "__main__":
    demo_go = (
            '{"metric": "a_importent_metric", "value": {"data": [{"ts": 0, "val": 33}'
            ', {"ts": 1, "val": 48}, {"ts": 2, "val": 74}, {"ts": 3, "val": 57}, {"ts":'
            '4, "val": 59}, {"ts": 5, "val": 12}]}}'
    )
    print(timeit.timeit(lambda: algo1_rust_version.rust_wrapper(demo_go), number=1000))


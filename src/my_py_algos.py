import pandas as pd
import timeit
import time
import json
from producer import GenericObject
class Algo1(object):

    """Algo substracts one of the metric"""
    def _calculation_inner(self, frame: pd.DataFrame):
        """Adds one to the whole dataframe
        :returns: dataframe

        """
        frame = frame + 1
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
    algo = Algo1()
    raw_data: dict = json.loads(demo_go)
    go = GenericObject.from_data(raw_data)
    now = time.time()
    for i in range(8076):
        algo.calcuation(go)
    end = time.time()
    print(f"Took {end-now} seconds")


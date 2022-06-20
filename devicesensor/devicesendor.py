import json
import random

import pandas as pd
import os


class DeviceSendor:
    def __init__(self):
        print("-----------------------Read the car sensor data-----------------------")
        self.df = pd.read_csv(os.path.join(os.path.dirname(__file__), "cleaned-car-sensor-data.csv"))

    def send(self):
        # print("hello how are you")
        # message = self.df[1].apply(lambda x: x.to_json(), axis=1)
        count = len(self.df)
        index = random.randrange(0, count)
        jdata = json.loads(self.df.iloc[index].to_json())
        print(jdata)

import os
import pandas as pd


root_dir = os.path.dirname(os.path.dirname(__file__))
csv_path = os.path.join(root_dir,"data","csv")

class Olist:

    def get_data(self):
        
       
        file_names = os.listdir(csv_path)
        file_names.remove('.keep')
        key_names = []

        for i in file_names:
           key_names.append(i.strip('olist').strip("dataset.csv").strip("_"))
        #breakpoint()
        data = {}
        for (x,y) in zip(file_names,key_names):
            data[y]=pd.read_csv(os.path.join(csv_path, x))
        #breakpoint()
        return data

    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")


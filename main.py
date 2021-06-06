from lithops import FunctionExecutor, Storage
import lithops
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker
from collections import OrderedDict

BUCKET_NAME = 'sd-datasets'
ENDPOINT = 'https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints'
NAMESPACE = 'evaldas.ramonas@estudiants.urv.cat_dev'
API_KEY = "iJ5yV_3jL9wgDCoJCUB4cHdgsmr1kKIBjpOy7aXY5bBy"
REGION = 'eu-de'
ACCESS_KEY = '90b716817d974159a710ec8b0d510377'
SECRET_KEY = 'ca6982a4e03fb75d7dba0401243c859196195c470bc5e815' 

KEY = 'datasetCasos.csv'

config = {
        'lithops' : {'storage_bucket' : BUCKET_NAME},

        'ibm_cf' : {'endpoint': ENDPOINT,
                    'namespace': NAMESPACE,
                    'api_key': API_KEY},

        'ibm_cos' : {'region': REGION,
                     'access_key': ACCESS_KEY,
                     'secret_key': SECRET_KEY}}
 

def get_file(key, config):
    dicRetorna = {}         # Diccionari per retornar
    counts = {'':''}

    storage = Storage(config=config)
    configs = config["lithops"]
    bucket = configs["storage_bucket"]
    fitxer = storage.get_object(bucket=bucket, key=key, stream=True)
    
    datos = pd.read_csv(fitxer)
    df = pd.DataFrame(datos)
    df['TipusCasData'] = pd.to_datetime(df['TipusCasData'], format= '%d/%m/%Y')     # Sort CSV per data
    df = df.sort_values(by = 'TipusCasData')

    counts=df['SexeDescripcio'].value_counts()      # GRAPH 1: Dones i homes infectats
    dicRetorna["graph1"] = counts
    counts=df['ComarcaDescripcio'].value_counts()   # GRAPH 2: Infectats per comarca
    dicRetorna["graph2"] = counts
    counts=df['TipusCasData'].value_counts()
    counts=OrderedDict(sorted(counts.items()))
    dicRetorna["graph3"] = counts                   # GRAPH 3: Infectats al llarg del temps

    return dicRetorna

if __name__ == '__main__':
    with FunctionExecutor() as fexec:
        fut = fexec.call_async(get_file, (KEY, config))
        #fexec.plot()
        dicRetorna = fut.result()

        #primer = False                             # En cas de que tots siguin de bara
        #for plot in dicRetorna:
        #    if primer:
        #        plt.figure()
        #    primer = True
        #    plt.bar(*zip(*dicRetorna[plot].items()))

        plt.bar(*zip(*dicRetorna["graph1"].items()))
        plt.ylabel('infectats', fontsize=16)
        plt.figure()
        plt.bar(*zip(*dicRetorna["graph2"].items()))
        plt.xticks(rotation=60) 
        plt.ylabel('infectats', fontsize=16)
        plt.figure()
        plt.plot(*zip(*dicRetorna["graph3"].items()))
        plt.gcf().autofmt_xdate()
        plt.xticks(rotation=35)
        plt.ylabel('infectats', fontsize=16)
        loc = matplotlib.ticker.LinearLocator(numticks = 14)
        plt.gca().xaxis.set_major_locator(loc)
        
        plt.show()
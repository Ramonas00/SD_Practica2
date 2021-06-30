from lithops import FunctionExecutor, Storage
import lithops
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker
from collections import OrderedDict

BUCKET = "sd-datasets"

def get_file(key1, key2):
    password = "csvCombinat.csv"

    storage = Storage()
    fitxer1 = storage.get_object(bucket=BUCKET, key=key1, stream=True)
    fitxer2 = storage.get_object(bucket=BUCKET, key=key2, stream=True)

    primer = pd.read_csv(fitxer1, usecols = ['TipusCasData','ComarcaDescripcio','SexeDescripcio'])
    primer.columns = ['Data','Comarca','Sexe']
    segon = pd.read_csv(fitxer2, usecols = ['NOM','DATA','SEXE'])
    segon = segon[['DATA','NOM','SEXE']]
    segon.columns = ['Data','Comarca','Sexe']
    combinat = primer.append(segon)
    filecsv = combinat.to_csv()

    storage.put_object(bucket=BUCKET, key=password, body=filecsv)

    return password


def generate_plots(key):
    dicRetorna = {}         # Diccionari per retornar
    counts = {'':''}

    storage = Storage()
    fitxer = storage.get_object(bucket=BUCKET, key=key, stream=True)
    
    df = pd.read_csv(fitxer)
    print(df)
    #df = pd.DataFrame(datos)
    df['Data'] = pd.to_datetime(df['Data'], format= '%d/%m/%Y')     # Sort CSV per data
    df = df.sort_values(by = 'Data')

    counts=df['Sexe'].value_counts()      # GRAPH 1: Dones i homes infectats
    dicRetorna["graph1"] = counts
    counts=df['Comarca'].value_counts()   # GRAPH 2: Infectats per comarca
    dicRetorna["graph2"] = counts
    counts=df['Data'].value_counts()
    counts=OrderedDict(sorted(counts.items()))
    dicRetorna["graph3"] = counts                   # GRAPH 3: Infectats al llarg del temps

    return dicRetorna

if __name__ == '__main__':
    KEY1 = 'datasetCasos.csv'
    KEY2 = 'datasetCasosComarca.csv'

    with FunctionExecutor() as fexec:
        fut = fexec.call_async(get_file, (KEY1, KEY2))
        KEY = fut.result()
        fut2 = fexec.call_async(generate_plots, KEY)
        dicRetorna = fut2.result()
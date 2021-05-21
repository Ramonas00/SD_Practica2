from lithops import FunctionExecutor, Storage
import lithops
import ibm_boto3
import pandas as pd
import matplotlib.pyplot as plt

BUCKET_NAME = 'sd-datasets'
ENDPOINT = 'https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints'
NAMESPACE = 'evaldas.ramonas@estudiants.urv.cat_dev'
API_KEY = "iJ5yV_3jL9wgDCoJCUB4cHdgsmr1kKIBjpOy7aXY5bBy"
REGION = 'eu-de'
ACCESS_KEY = '90b716817d974159a710ec8b0d510377'
SECRET_KEY = 'ca6982a4e03fb75d7dba0401243c859196195c470bc5e815' 

KEY = 'dataset3.csv'

config = {
        'lithops' : {'storage_bucket' : BUCKET_NAME},

        'ibm_cf' : {'endpoint': ENDPOINT,
                    'namespace': NAMESPACE,
                    'api_key': API_KEY},

        'ibm_cos' : {'region': REGION,
                     'access_key': ACCESS_KEY,
                     'secret_key': SECRET_KEY}}


def get_file(key, config):
    storage = Storage(config=config)
    fitxer = storage.get_object(bucket=BUCKET_NAME, key=key, stream=True)
    
    datos = pd.read_csv(fitxer)
    df = pd.DataFrame(datos)

    df.groupby("ComarcaDescripcio")["NumCasos"].sum().plot(kind='barh',legend='Reverse')

if __name__ == '__main__':
    with FunctionExecutor() as fexec:
        fut = fexec.call_async(get_file, (KEY, config))
        #print(fut.result())
        plt.show()
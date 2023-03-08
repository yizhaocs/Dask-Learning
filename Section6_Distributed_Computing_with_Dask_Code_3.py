'''
Exploring Dask Dashboard UI
'''
from dask.distributed import Client
import dask.dataframe as df

client = Client(processes=False, threads_per_worker=2, n_workers=3, memory_limit='4GB')
print(f"client:{client}")
'''
Data Taken from:  https://www.kaggle.com/datasets/city-of-seattle/seattle-library-collection-inventory
'''
dummy_df = df.read_csv('resources/data/library-collection-inventory.csv')
print(f"dummy_df:{dummy_df}")
print(f"dummy_df.describe().compute():{dummy_df.describe().compute()}")
print(f"dummy_df.head():{dummy_df.head()}")



# Currently in 1.2.1 inplace argument is not supported
# dummy_df = dummy_df.drop('Unnamed: 0', axis=1)
print(f"dummy_df.head():{dummy_df.head()}")
max_gdp_per_country = dummy_df.groupby('PublicationYear').count()
print(f"max_gdp_per_country.compute():{max_gdp_per_country.compute()}")
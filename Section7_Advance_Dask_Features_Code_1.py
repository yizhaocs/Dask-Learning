from dask.distributed import Client
import dask.dataframe as df

client = Client(processes=False, threads_per_worker=2, n_workers=3, memory_limit='4GB')
print(f"client:{client}")

dummy_df = df.read_csv("resources/data/Countries-GDP-1960-2020.csv")
print(f"dummy_df.head():{dummy_df.head()}")

'''
Visualize the task graph
'''
dummy_df.describe().visualize(filename='output/describe.png')

max_gdp_per_country = dummy_df.groupby('Country Name')['2000'].max()
print(f"max_gdp_per_country.compute(): {max_gdp_per_country.compute()}")
'''
https://cocalc.com/share/public_paths/6047eba56ed510d3f13ec86819558896e39decef

'''
from dask.distributed import Client, LocalCluster
import dask.array as da

cluster = LocalCluster(
    n_workers=3,
    threads_per_worker=1,
    processes=True,
    diagnostics_port=None,
)
client = Client(cluster)
x = da.random.random((300, 300), chunks=(10, 10))
y = x + x.T
z = (y.mean(axis=1) / y.shape[0]).std()
print(z.compute())
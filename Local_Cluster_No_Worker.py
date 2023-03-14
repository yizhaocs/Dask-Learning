'''
https://cocalc.com/share/public_paths/6047eba56ed510d3f13ec86819558896e39decef

'''
from dask.distributed import Client, LocalCluster
import dask.array as da
import dask
import os

if __name__ == '__main__':
    # dask.config.set({
    #     'temporary_directory': os.path.expanduser('~/tmp'),
    #     'scheduler.work-stealing': True
    # })

    client = Client()
    print(f"client:{client}")
    x = da.random.random((300, 300), chunks=(10, 10))
    y = x + x.T
    z = (y.mean(axis=1) / y.shape[0]).std()
    print(z.compute())
    client.close()
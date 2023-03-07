import numpy as np
import dask.array as da

'''
Dask Arrays Interface is similar to Numpy API
'''
np_arr = np.random.randint(20, size=20)
print(f"np_arr: {np_arr}")

'''
In this case, we just need to add an additional attribute chunks
'''
dask_arr = da.random.randint(20, size=20, chunks=3)
# dask_arr: dask.array<randint, shape=(20,), dtype=int64, chunksize=(3,), chunktype=numpy.ndarray>
print(f"dask_arr: {dask_arr}")

'''
This is simply because Dask does lazy evaluation.
You need to call compute() to start the execution
'''
print(f"dask_arr.compute(): {dask_arr.compute()}")

'''
Chunks size
'''
print(f"dask_arr.chunks: {dask_arr.chunks}")

'''
Creating arrays from existing data
'''
dask_arr_from_np = da.from_array(np_arr, chunks=5)
print(f"dask_arr_from_np: {dask_arr_from_np}")
print(f"dask_arr_from_np.compute(): {dask_arr_from_np.compute()}")

# '''
# Dask translates your array operations into a graph to
# '''
# dask_arr_from_np.sum().visualize()
# dask_arr_from_np.sum().visualize(rankdir="LR")
#
# dask_arr_mean = da.mean(dask_arr_from_np)
# dask_arr_mean.compute()
#
# '''
# Another Example
# '''
# x = da.random.random(10, chunks=2)
from dask.distributed import Client
import dask.dataframe as df

client = Client(processes=False, threads_per_worker=2, n_workers=3, memory_limit='4GB')
print(f'client:{Client}')

import time
def some_func(x):
    time.sleep(1)
    y = x + 1
    time.sleep(1)
    z = x * y
    time.sleep(1)
    return z

'''
Client.submit returns a Future Object
'''
result_1 = client.submit(some_func, 5)
print(f'result_1:{result_1}')

'''
In this case the computation will be pending
'''
def some_func_long(x):
    time.sleep(10)
    y = x + 1
    time.sleep(5)
    z = x * y
    time.sleep(2)
    return z

result_2 = client.submit(some_func_long, 5)
print(f'result_2:{result_2}')


'''
result() function can be used to force the computation to complete
'''
result_1.result()
result_2.result()

'''
We can evaluate multiple Future objects in one go using gather
'''
futures_list = [client.submit(some_func, x) for x in range(1, 5)]
all_res = client.gather(futures_list)
print(f'all_res:{all_res}')


'''
We can also use map function to pass multiple future object
'''
list_result = client.map(some_func_long, range(5))
print(f'client.gather(list_result):{client.gather(list_result)}')

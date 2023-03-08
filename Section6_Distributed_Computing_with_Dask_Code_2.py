from dask import delayed, compute
import dask
import time


x = list(range(2, 20000, 2))
y = list(range(3, 30000, 3))
z = list(range(5, 50000, 5))

final_result = []


def do_something_1(x, y):
    return x + y + 2 * x * y


def do_something_2(a, b):
    return a ** 3 - b ** 3


def do_something_3(p, q):
    return p * p + q * q

final_result_for_dask_delayed = []

for i in range(0, len(x)):
    res_1 = delayed(do_something_1)(x[i], y[i])
    res_2 = delayed(do_something_2)(y[i], z[i])
    res_3 = delayed(do_something_3)(res_1, res_2)
    final_result_for_dask_delayed.append(res_3)

final_sum = delayed(sum)(final_result)

with dask.config.set(scheduler='threading'):
    start_time = time.perf_counter()
    final_sum.compute()
    end_time = time.perf_counter()
    print("Execution time for scheduler='threading':", end_time - start_time, "seconds")

with dask.config.set(scheduler='sync'):
    start_time = time.perf_counter()
    final_sum.compute()
    end_time = time.perf_counter()
    print("Execution time for scheduler='sync':", end_time - start_time, "seconds")

# with dask.config.set(scheduler='processes'):
#     start_time = time.perf_counter()
#     final_sum.compute()
#     end_time = time.perf_counter()
#     print("Execution time for scheduler='processes':", end_time - start_time, "seconds")


'''
Serial Execution
'''


def do_something_1(x, y):
    return x + y + 2 * x * y


def do_something_2(a, b):
    return a ** 3 - b ** 3


def do_something_3(p, q):
    return p * p + q * q


x = [2, 4, 6, 8, 10]
y = [3, 6, 9, 12, 15]
z = [10, 20, 30, 40, 50]

final_result = []

for i in range(0, len(x)):
    res_1 = do_something_1(x[i], y[i])
    res_2 = do_something_2(y[i], z[i])
    res_3 = do_something_3(res_1, res_2)
    final_result.append(res_3)

# final_result:[947018, 60594020, 690180570, 3877846928, 14792746250]
print(f'final_result:{final_result}')
# sum(final_result):19422314786
print(f'sum(final_result):{sum(final_result)}')

'''
Using dask.delayed
'''
from dask import delayed, compute
final_result_for_dask_delayed = []
for i in range(0, len(x)):
    # Wrap the function calls inside delayed
    res_1 = delayed(do_something_1)(x[i], y[i])
    res_2 = delayed(do_something_2)(y[i], z[i])
    res_3 = delayed(do_something_3)(res_1, res_2)
    final_result_for_dask_delayed.append(res_3)

final_sum = delayed(sum)(final_result)

'''
Lazily evaluated
'''
# final_sum:Delayed('sum-d3aa31b1-c3d6-479e-8ea4-244833a9916c')
print(f'final_sum:{final_sum}')

'''
Call compute to execute
'''
# final_sum.compute():19422314786
print(f'final_sum.compute():{final_sum.compute()}')


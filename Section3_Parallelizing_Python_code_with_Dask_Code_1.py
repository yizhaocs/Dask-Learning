'''
Eager Evaluation
'''


def my_func(a, b):
    return a * b + 3


result = my_func(10, 20)
# result:203
print(f'result:{result}')

'''
Lazy Evaluation using Generators
'''
def my_lazy_func(a, b):
    a = a + 2
    b = b * 10
    yield a, b

lazy_stuff = my_lazy_func(5, 8)
# lazy_stuff:<generator object my_lazy_func at 0x7ff8ceef7af0>
print(f'lazy_stuff:{lazy_stuff}')

'''
Evaluate only when needed
'''
# next(lazy_stuff): (7, 80)
print(f'next(lazy_stuff): {next(lazy_stuff)}')


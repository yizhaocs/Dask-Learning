'''
Using Dask-ML for Regression
'''
from dask.distributed import Client, progress


client = Client(processes=False, threads_per_worker=4, n_workers=3, memory_limit='2GB')
print(f'client:{client}')

from sklearn.datasets import make_regression
from sklearn.linear_model import LinearRegression, Ridge

X, y = make_regression(n_samples=10000, random_state=0, n_features=10, n_informative=6)

print(f'len(X[0]):{len(X[0])}')
print(f'X:{X}')

clf = LinearRegression()
# clf.fit(X, y)
#
# clf2 = Ridge()
# clf2.fit(X, y)

import joblib

with joblib.parallel_backend('dask'):
    clf.fit(X, y)

print(f'clf.predict(X)[:5]:{clf.predict(X)[:5]}')
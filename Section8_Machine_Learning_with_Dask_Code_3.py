'''
Using Dask-ML for Hyper-Parameter Tuning
'''
from dask.distributed import Client, progress

# If you want to run workers in your same process, you can pass the processes=False keyword argument.
# client = Client(processes=True)
client = Client(processes=False, threads_per_worker=4, n_workers=3, memory_limit='2GB')
print(f"client:{client}")

from sklearn.svm import SVC
from sklearn.datasets import make_classification
from sklearn.model_selection import GridSearchCV

X, y = make_classification(n_samples=500, random_state=0, n_classes=3, n_features=5, n_informative=3, n_redundant=2)
print(f'X[0]:{X[0]}')
print(f'X[:2]:{X[:2]}')
param_grid = {
    "C": [0.00001, 0.0001, 0.001, 0.01, 0.1, 1],
    "kernel": ['rbf', 'poly', 'sigmoid'],
    "degree": [1, 2, 3, 4],
    "coef0": [1, 0.5, 0.3, 0.2, 0.1],
    "gamma": ["auto", "scale"]
}

clf = SVC(random_state=0, probability=True)
grid_search = GridSearchCV(clf, param_grid=param_grid, cv=3, n_jobs=1)

'''
不用dask-ml来tune sklearn
'''
import time

start_time = time.perf_counter()
grid_search.fit(X, y)

end_time = time.perf_counter()

# Execution time: 47.798580993985524 seconds
print("Execution time:", end_time - start_time, "seconds")


'''
用dask-ml来tune sklearn
'''
import joblib

start_time = time.perf_counter()
with joblib.parallel_backend('dask'):
    grid_search.fit(X, y)

end_time = time.perf_counter()

# Execution time: 12.109803343002568 seconds
print("Execution time:", end_time - start_time, "seconds")

print(f"grid_search.predict(X)[:10]:{grid_search.predict(X)[:10]}")

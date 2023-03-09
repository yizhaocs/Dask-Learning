'''
Using Dask-ml for Classification
'''
from dask_ml.cluster import KMeans
import dask_ml.datasets

'''
You can create larger than memory dataset
'''
X, y = dask_ml.datasets.make_blobs(n_samples=500000, chunks=50000, random_state=0, centers=5)

clf = KMeans(n_clusters=5, init_max_iter=20)
clf.fit(X)

my_pred = clf.predict(X[:10])
print(f'my_pred.compute():{my_pred.compute()}')

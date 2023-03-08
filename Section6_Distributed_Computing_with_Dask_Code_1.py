from dask.distributed import Client

client = Client(processes=False, threads_per_worker=2, n_workers=3, memory_limit='2GB')
# client:<Client: 'inproc://172.30.172.130/62290/1' processes=3 threads=6, memory=6.00 GB>
print(f'client:{client}')
print(f'client.status:{client.status}')
client.close()
print(f'client.status:{client.status}')

from dask.distributed import LocalCluster

my_cluster = LocalCluster()
my_client = Client(my_cluster)

new_worker = my_cluster.start_worker(ncores=3)
my_cluster.stop_worker(new_worker)
print(f'my_cluster.status(): {my_cluster.status()}')
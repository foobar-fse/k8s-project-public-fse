#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

# the nfs size is 2.0 GB in total, we use 12 cores for it
cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 12 --executor-cores 4'

# for small dataset-2020
nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/data-collection-20201208/172.16.112.85/storage-state/persistent-disk/*/'
nfsPathOut = 'hdfs://spark5-master:9000/user/k8s/nfs-keep-first-last-total-8/'

# we collect the persistent-disk (or NFS) data in 172.16.112.85
# for one-year dataset, 2023-01~2023-12
# k8s_nfs_cluster1/112/113/19? ignore k8s_nfs_cluster_unknown?
#nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/k8s-data-2022-foobar/node85/storage-state/persistent-disk/k8s_nfs_cluster1*/*/'

#nfsPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/nfs-keep-first-last-total/'


cmd = cmdPrefix + ' keep_first_last_nfs.py ' + nfsPathIn + ' ' + nfsPathOut

start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)


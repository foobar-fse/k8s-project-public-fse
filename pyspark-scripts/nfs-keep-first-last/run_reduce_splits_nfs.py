#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

# to not consume entire cluster resource, we use 50 executors
# to avoid out-of-memory, we use 18g (not 20g) for each executor 

# only about 150 MB to reduce

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 18g  --num-executors 12 --executor-cores 4'


nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/nfs-remove-duplicates-keep-last-split-8/2023-*/'

nfsPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/nfs-remove-duplicates-keep-last-total-8/'

cmd = cmdPrefix + ' reduce_splits_nfs.py ' + nfsPathIn + ' ' + nfsPathOut

start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)


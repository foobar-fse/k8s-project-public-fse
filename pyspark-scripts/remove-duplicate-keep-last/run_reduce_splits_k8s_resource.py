#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

# to not consume entire cluster resource, we use 50 executors
# to avoid out-of-memory, we use 18g (not 20g) for each executor 

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 18g  --num-executors 50 --executor-cores 4'


#rescPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/test-splits/2023-01-*/'

#rescPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-split/2023-*/'

rescPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-split-8/2023-*/'

#rescPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/test-combine/'

#rescPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-total/'

rescPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-total-8/'

cmd = cmdPrefix + ' reduce_splits_k8s_resource.py ' + rescPathIn + ' ' + rescPathOut


start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)

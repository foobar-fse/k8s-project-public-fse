#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

# remove-duplicate-total takes a long time, we can use 60 executors 

# we can not process the 4TB dataset in a single run, so we split it by month 
# the local disk for hdfs-tmp directory is not enough, we only have 300~800GB
# and the memory for executor if set 20g will OOM, we have to use 18g executor 

# the following specification (20g * 60) can process a dataset (500~800GB) for a month (except Feburary)  

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 60 --executor-cores 4'

#rescPathIn = 'hdfs://spark5-master:9000/user/k8s/data-collection-20201208/172.16.112.64/k8s-api-resource/2020-12-08_19*/'
rescPathIn = 'hdfs://spark5-master:9000/user/k8s/data-collection-20201208/172.16.112.64/k8s-api-resource/*/'

#rescPathIn = 'hdfs://spark5-master:9000/user/k8s/k8s-data-2022-foobar/node4/k8s-api-resource/'

#rescPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231113/k8s-api-resource-remove-duplicates-keep-last/'
rescPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231120/k8s-api-resource-remove-duplicates-keep-last-total-8/'

#rescPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-total/'

cmd = cmdPrefix + ' remove_duplicate_keep_last_k8s_resource.py ' + rescPathIn + ' ' + rescPathOut

start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)


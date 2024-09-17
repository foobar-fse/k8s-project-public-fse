#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 40 --executor-cores 4'

#rescPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231113/k8s-api-resource-remove-duplicates-keep-last/'
rescPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231120/k8s-api-resource-remove-duplicates-keep-last-total/'
#rescPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-split/2023-03/'
#rescPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-total/'


#tagPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231113/kind-key-tag.csv'
#tagPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231120/kind-key-tag.csv'
tagPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231120/kind-key-tag-2/'
#tagPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/kind-key-tag-2023/'

#pathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231113/k8s-api-resource-get-entity/'
pathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231120/k8s-api-resource-get-entity-total-8/'
#pathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-get-entity-split/2023-03/'
#pathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-get-entity-total/'


cmd = cmdPrefix + ' extract_canonical_entity.py ' + rescPathIn + ' ' + tagPathIn + ' ' + pathOut

start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)



#!/usr/bin/env python
  
import os
import sys
import time 

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 40 --executor-cores 4'

k8sPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231113/k8s-api-resource-create-vertex-edge/'
#pathIn = 'hdfs://spark5-master:9000/user/k8s/test/k8s-api-resource-create-vertex-edge-total/'
nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231113/nfs-create-vertex-edge-2/'

pathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231113/k8s-nfs-convert-neo4j-csv/'
#pathOut = 'hdfs://spark5-master:9000/user/k8s/test/k8s-api-resource-convert-neo4j-csv-total/'
withReverse = False

cmd = cmdPrefix + ' build_k8s_nfs_neo4j_csv.py ' + k8sPathIn + ' ' + nfsPathIn + ' ' + pathOut + ' ' + str(withReverse)


start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)

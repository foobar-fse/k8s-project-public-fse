#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 40 --executor-cores 4  --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12'

#k8sPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231113/k8s-api-resource-create-vertex-edge/'
#nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231113/nfs-create-vertex-edge-2/'

k8sPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231120/k8s-api-resource-create-vertex-edge-total-8/'
nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231120/nfs-create-vertex-edge-total-8/'

#k8sPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-create-vertex-edge-split/2023-03/'
#nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/nfs-create-vertex-edge-split/2023-03/'

#k8sPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-create-vertex-edge-total/'
#nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/nfs-create-vertex-edge-total/'


#graphPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231113/entityGraph_json_2'
#metagraphPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231113/metaGraph_json_2'

graphPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231120/graphframe-build-graph-total-8/'
metagraphPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231120/graphframe-build-metagraph-total-8/'

#graphPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/graphframe-build-graph-split/2023-03/'
#metagraphPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/graphframe-build-metagraph-split/2023-03/'

#graphPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/graphframe-build-graph-total/'
#metagraphPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/graphframe-build-metagraph-total/'


# test hdfs expanding capacity
#graphPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231120/graphframe-build-graph-total-3/'
#metagraphPathOut = 'hdfs://spark5-master:9000/user/k8s/test-20231120/graphframe-build-metagraph-total-3/'


cmd = cmdPrefix + ' build_graph_metagraph.py ' + k8sPathIn + ' ' + nfsPathIn + ' ' + graphPathOut + ' ' + metagraphPathOut

start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)

#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 40 --executor-cores 4  --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12'


graphPathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/graphframe-build-graph-total/'
#graphPathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/graphframe-build-graph-total-fix/'
graphPathOut = graphPathIn


cmd = cmdPrefix + ' fix_namespace_overlap.py ' + graphPathIn + ' ' + graphPathOut

start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)

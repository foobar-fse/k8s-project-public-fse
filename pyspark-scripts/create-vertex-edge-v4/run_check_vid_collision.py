#!/usr/bin/env python
  
import os
import sys
import time

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 40 --executor-cores 4'

#pathIn = 'hdfs://spark5-master:9000/user/k8s/test/k8s-api-resource-create-vertex-edge/'
pathIn = 'hdfs://spark5-master:9000/user/k8s/test-20231120/k8s-api-resource-create-vertex-edge-total-8/'
#pathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-create-vertex-edge-split/2023-03/'
#pathIn = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-create-vertex-edge-total/'



#pathOut = 'hdfs://spark5-master:9000/user/k8s/test/k8s-api-resource-check-vid-collision/'
pathOut = 'hdfs://spark5-master:9000/user/k8s/test/k8s-api-resource-check-vid-collision-total-8/'
#pathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-check-vid-collision-split/2023-03/'
#pathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-check-vid-collision-total/'


cmd = cmdPrefix + ' check_vid_collision.py ' + pathIn + ' ' + pathOut

start_time = time.time()
print(cmd)
os.system(cmd)
time_cost = time.time() - start_time

print('COMPLETE in %.2f seconds.' % time_cost)

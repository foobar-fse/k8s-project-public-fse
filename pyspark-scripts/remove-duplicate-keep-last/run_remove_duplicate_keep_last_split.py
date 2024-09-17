#!/usr/bin/env python

'''
Remove consective duplicated snapshots and keep the last snapshot.
We cannot process 4TB data in one run, so we split it in smaller parts
'''

import os
import sys
import time
from datetime import datetime

# We have 24 containers with (6-core-32g-RAM) and 48 containers with (4-core-24g-RAM)
# For graceful running, we can use about half of the resources
# The concurrent level (determined by --executor-cores) should be [2, 4], not greater than 5, to avoid hurting HDFS performance


# we test 20g * 60, 20g * 50 executors, it will not success for Feburary 
# then we use 18g * 50 executor, it will not consume entire cluster resources and can avoid out-of-memory 

cmdPrefix = ('/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client '
             '--driver-memory 20g --executor-memory 18g  --num-executors 50 --executor-cores 4')

rescBasePathIn = 'hdfs://spark5-master:9000/user/k8s/k8s-data-2022-foobar/node4/k8s-api-resource/'
rescBasePathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/k8s-api-resource-remove-duplicates-keep-last-split-8/'

#rescBasePathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/test-splits/'

start_time = time.time()

# we have collected data from Jan to June, but missed those from July to December
for i in range(1, 6):  # take 06 as example
#for i in range(12, 13):
    rescPathIn = rescBasePathIn + '/2023-%02d-*/' % i
    rescPathOut = rescBasePathOut + '/2023-%02d/' % i
    
    #rescPathIn = rescBasePathIn + '/2023-01-%02d_*/' % i
    #rescPathOut = rescBasePathOut + '/2023-01-%02d/' % i
    
    cmd = cmdPrefix + ' remove_duplicate_keep_last_k8s_resource.py ' + rescPathIn + ' ' + rescPathOut

    t0 = time.time()

    print cmd
    os.system(cmd)
    
    #time.sleep(5)
    t1 = time.time()
    print "In round %d, time elapsed: %.2f seconds" % (i, t1 - t0)

end_time = time.time()
time_elapsed = end_time - start_time
end_time_formatted = datetime.fromtimestamp(end_time).strftime('%Y/%m/%d %H:%M:%S')

print "Time elapsed: %.2f seconds" % time_elapsed
print "End time: %s" % end_time_formatted

print 'COMPLETE'

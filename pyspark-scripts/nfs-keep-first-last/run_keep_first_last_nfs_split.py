#!/usr/bin/env python
  
import os
import sys
import time
from datetime import datetime

# we have 24 containers with (6-core-32g-ram) and 48 containers with (4-core-24g-ram)
# for grace running, we can use about half of the resources
# the concurrent-level (determinted by --executor-cores) should be [2, 4], not greater than 5, otherwise hurt HDFS performance

# the total size is small (2.0 GB), we use 12 cores 

cmdPrefix = '/usr/local/spark-3.1.2/bin/spark-submit  --master yarn --deploy-mode client --driver-memory 20g --executor-memory 20g  --num-executors 12 --executor-cores 4'

# we collect the persistent-disk (or NFS) data in 172.16.112.85
# k8s_nfs_cluster1/112/113/19? ignore k8s_nfs_cluster_unknown?

#nfsPathIn = 'hdfs://spark5-master:9000/user/k8s/k8s-data-2022-foobar/node85/storage-state/persistent-disk/k8s_nfs_cluster1/2023-01-10_02.24.29.010

nfsBasePathIn = 'hdfs://spark5-master:9000/user/k8s/k8s-data-2022-foobar/node85/storage-state/persistent-disk/k8s_nfs_cluster1*/'

nfsBasePathOut = 'hdfs://spark5-master:9000/user/k8s/large-20231221/nfs-remove-duplicates-keep-last-split-8/'

start_time = time.time()

# for one-year dataset, 2023-01~2023-12
for i in range(1, 13):
    nfsPathIn = nfsBasePathIn + '/2023-%02d-*/' % i
    nfsPathOut = nfsBasePathOut + '/2023-%02d/' % i

    cmd = cmdPrefix + ' keep_first_last_nfs.py ' + nfsPathIn + ' ' + nfsPathOut

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


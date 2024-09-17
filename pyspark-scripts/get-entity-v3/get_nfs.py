# get nfs from Pod and PersistentVolume

from pyspark.sql.functions import *

#nfsTag = spark.read.csv(testPath + '/nfs.csv', header=True)
#nfsResc = rescFlatDF3.join(nfsTag, ['kind', 'key'], 'inner')

def get_nfs(nfsResc):
    nfsPath = nfsResc.filter(col('tag2') == 'path').withColumnRenamed('val', 'path')
    nfsServer = nfsResc.filter(col('tag2') == 'server').withColumnRenamed('val', 'server')

    # we only care about metadataUid, timestamp(optional), prefix and server 
    tmpServer = nfsServer.select('metadataUid', 'timestamp', 'prefix', 'server').distinct()
    nfsMatch = nfsPath.join(tmpServer, ['metadataUid', 'timestamp', 'prefix'], 'inner')
    
    return nfsMatch

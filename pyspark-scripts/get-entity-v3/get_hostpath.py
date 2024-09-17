# get hostPath from Pod

from pyspark.sql.functions import *

# use rescFlatDF3 in get_image.py

#hostPathTag = spark.read.csv(testPath + '/hostPath.csv', header=True)
#hostPathResc = rescFlatDF3.join(hostPathTag, ['kind', 'key'], 'inner')

def get_hostpath(hostPathResc):
    hpPath = hostPathResc.filter(col('tag2') == 'path').withColumnRenamed('val', 'path')
    hpServer = hostPathResc.filter(col('tag2') == 'server').withColumnRenamed('val', 'server')

    # we only care metadataUid, timestamp(optional), and server 
    tmpServer = hpServer.select('metadataUid', 'timestamp', 'server').distinct()
    hpMatch = hpPath.join(tmpServer, ['metadataUid', 'timestamp'], 'inner')
    
    return hpMatch

# we avoid multiple edges with timestamp and key
# (1) merge timestamp of same src and dest, and get [tmin, tmax]
# (2) drop prefix to ignore multiple edges with same key

import sys
import json, re
from pyspark.sql.functions import *
from pyspark.sql import SparkSession



# main function 
if __name__ == "__main__":
    """
        Usage: aviod_multiple_edge.py [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonAvoidMultipleEdge")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]
    
    #'''
    # container
    print ('merge timestamp and drop prefix for container ...')
    container = spark.read.json(pathIn + '/container')
    ctrTime = container.groupBy('metadataUid', 'key', 'containerName', 'containerID')\
                    .agg(min('timestamp').alias('tmin'),  max('timestamp').alias('tmax'))\
                    .withColumnRenamed('metadataUid', 'metadataUid2')\
                    .withColumnRenamed('key', 'key2')\
                    .withColumnRenamed('containerName', 'containerName2')\
                    .withColumnRenamed('containerID', 'containerID2')
    
    ctrKeyList = [x for x in container.schema.names if x not in ['timestamp', 'prefix']] 
    ctrKeyList.extend(['tmin', 'tmax'])
    ctrRes = container.join(ctrTime, (container.metadataUid == ctrTime.metadataUid2)\
                                & (container.key == ctrTime.key2)\
                                & (container.containerID.eqNullSafe(ctrTime.containerID2))\
                                & (container.containerName.eqNullSafe(ctrTime.containerName2)), 'inner')\
                    .select(ctrKeyList)\
                    .distinct()

    ctrRes.write.json(pathOut + '/container')
    #'''
    #'''
    # image
    print ('merge timestamp and drop prefix for image ...')
    image = spark.read.json(pathIn + '/image')
    imageTime = image.groupBy('metadataUid', 'key', 'imageName', 'imageID')\
                    .agg(min('timestamp').alias('tmin'), max('timestamp').alias('tmax'))\
                    .withColumnRenamed('metadataUid', 'metadataUid2')\
                    .withColumnRenamed('key', 'key2')\
                    .withColumnRenamed('imageName', 'imageName2')\
                    .withColumnRenamed('imageID', 'imageID2')
   
    imgKeyList = [x for x in image.schema.names if x not in ['timestamp', 'prefix']]
    imgKeyList.extend(['tmin', 'tmax'])
    imageRes = image.join(imageTime, (image.metadataUid == imageTime.metadataUid2)\
                                & (image.key == imageTime.key2)\
                                & (image.imageID.eqNullSafe(imageTime.imageID2))\
                                & (image.imageName.eqNullSafe(imageTime.imageName2)), 'inner')\
                    .select(imgKeyList)\
                    .distinct()
    
    imageRes.write.json(pathOut + '/image')
    #'''
    #'''
    # hostpath
    print ('merge timestamp and drop prefix for hostpath ...')
    hostpath = spark.read.json(pathIn + '/hostpath')
    hpTime = hostpath.groupBy('metadataUid', 'key', 'path', 'server')\
                    .agg(min('timestamp').alias('tmin'), max('timestamp').alias('tmax'))
    # path and server are non-null                 
    hpRes = hostpath.drop('timestamp', 'prefix')\
                    .join(hpTime, ['metadataUid', 'key', 'path', 'server'], 'inner')\
                    .distinct()
    hpRes.write.json(pathOut + '/hostpath')
    #'''

    # nfs
    print ('merge timestamp and drop prefix for nfs ...')
    nfs = spark.read.json(pathIn + '/nfs')
    nfsTime = nfs.groupBy('metadataUid', 'key', 'path', 'server')\
                .agg(min('timestamp').alias('tmin'), max('timestamp').alias('tmax'))
    # path and server are non-null   
    nfsRes = nfs.drop('timestamp', 'prefix')\
                .join(nfsTime, ['metadataUid', 'key', 'path', 'server'], 'inner')\
                .distinct()
    nfsRes.write.json(pathOut + '/nfs')
    
    # atomic
    print ('merge timestamp and drop prefix for atomic ...')
    atomic = spark.read.json(pathIn + '/atomic')
    atomicTime = atomic.groupBy('metadataUid', 'key', 'val')\
                        .agg(min('timestamp').alias('tmin'), max('timestamp').alias('tmax'))
    # val is non-null
    atomicRes = atomic.drop('timestamp', 'prefix')\
                        .join(atomicTime, ['metadataUid', 'key', 'val'], 'inner')\
                        .distinct()
    
    atomicRes.write.json(pathOut + '/atomic')
    
    # k8s-native
    print ('merge timestamp and drop prefix for k8s-native ...') 
    native = spark.read.json(pathIn + '/k8s-native')
    # we can not use desthash = HASH(kind2, name2, namespace2, uid2) which will introduce collisions
    # no matter hash(), md5(), sha2() is used
    # [kind2, name2, namespace2, uid2] can uniquely determine the entity, selfLink2 can be omitted
    # but [name2, namespace2, uid2] can not determine
    # namespace2 and uid2 can be null, kind2 and name2 are non-null 
    nativeTime = native.groupBy('metadataUid', 'key', 'kind2', 'name2', 'namespace2', 'uid2')\
                        .agg(min('timestamp').alias('tmin'), max('timestamp').alias('tmax'))\
                        .withColumnRenamed('namespace2', 'namespace3')\
                        .withColumnRenamed('uid2', 'uid3')
    # join then filter to avoid many renaming
    # in this case, the 4-key join produces a unique result, and the latter eqNullSafe() is always true  
    nativeRes = native.join(nativeTime, ['metadataUid', 'key', 'kind2', 'name2'], 'inner')\
                        .filter(col('namespace2').eqNullSafe(col('namespace3')))\
                        .filter(col('uid2').eqNullSafe(col('uid3')))\
                        .drop('namespace3', 'uid3', 'timestamp', 'prefix')\
                        .distinct()

    nativeRes.write.json(pathOut + '/k8s-native')
        
    spark.stop()

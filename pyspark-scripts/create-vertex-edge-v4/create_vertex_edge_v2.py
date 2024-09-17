# create edge and vertex from rows
# note: don't overlap fields in the k8s-api-resource, their schema.names will be merged in vertex
# we simplify edge types with ReferInternal and UseExternal, and use srcKind, destKind to annotate  

import sys
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


udf_uuid5 = udf(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)), StringType())


# main function
if __name__ == "__main__":
    """
        Usage: create_vertex_edge.py [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonCreateVertexEdge")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]

    ## k8s-native
    print('create vertex and edge for k8s-native ...')
    
    native = spark.read.json(pathIn + '/k8s-native')
    # we use isDetermined which only requires the uid2 is not null
    # the isCanonical requires uid2, name2, namespace2 and kind2 are all clearly known
    native2 = native.withColumn('desthash', when(col('uid2').isNotNull(), col('uid2'))\
                                            .otherwise(udf_uuid5(concat_ws('/', 'name2', 'namespace2', 'kind2'))))\
                    .withColumn('isDetermined', col('uid2').isNotNull())\
                    .cache()

    # source vertices are all from metadata
    # note: don't rename the 'uid2' as 'uid', 
    # otherwise it will overlap with the k8s-api-resource 'uid' in the later vertex schema union
    nativeSrc = native2.filter(col('tag') == 'metadata')\
                        .select('uid2', 'name2', 'namespace2', 'kind2', 'selfLink2', col('desthash').alias('vid'))\
                        .withColumn('isAtomic', lit(False))\
                        .withColumn('isNative', lit(True))\
                        .withColumn('isDetermined', lit(True))\
                        .distinct()

    nativeDest = native2.filter(col('tag') != 'metadata')\
                        .select('uid2', 'name2', 'namespace2', 'kind2', 'selfLink2', col('desthash').alias('vid'),\
                                'isAtomic', 'isNative', 'isDetermined')\
                        .distinct()
    # for k8s interal resources, we set edge type as ReferInternal, and use kind as srcKind, kind2 as destKind
    nativeEdge = native2.filter(col('tag') != 'metadata')\
                        .withColumn('type', lit('ReferInternal'))\
                        .withColumn('srcKind', col('kind'))\
                        .withColumn('destKind', col('kind2'))\
                        .select(col('metahash').alias('srcVid'), col('desthash').alias('destVid'),\
                                'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')\
                        .distinct()

    # add edge to namespace
    # we can confirm that {metadataUid} is a subset of {uid2}
    # because every k8s api-resource has metadata, and we view it as the central entity
    # and the metadata_uid line will capture this relation

    y1 = native2.select('desthash', 'uid2', 'name2', 'namespace2', 'kind2').distinct()
    y2 = native2.groupBy('desthash').agg(min('tmin').alias('tmin'), max('tmax').alias('tmax'))

    y3 = y1.join(y2, 'desthash', 'left').cache()
    y4 = y3.filter(col('kind2') != 'Namespace')
    ns = y3.filter(col('kind2') == 'Namespace')\
            .select(col('desthash').alias('desthash3'), col('name2').alias('name3'), col('kind2').alias('kind3'))
    y5 = y4.join(ns, y4.namespace2 == ns.name3, 'left')

    # we use ReferInternal for convenience, without changes in down-stream processing
    # or, we can use HasNamespace as special edge
    # system-level resources don't have namespace, we will filter out these rows
    # otherwise, the neo4j import will have many missing node
    nativeNsEdge = y5.withColumn('type', lit('ReferInternal'))\
                    .withColumn('key', lit('metadata_namespace'))\
                    .withColumnRenamed('kind2', 'srcKind')\
                    .withColumnRenamed('kind3', 'destKind')\
                    .withColumnRenamed('desthash', 'srcVid')\
                    .withColumnRenamed('desthash3', 'destVid')\
                    .select('srcVid', 'destVid', 'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')\
                    .filter(col('destVid').isNotNull())

    # write srcVertex, destVertex and edge 
    nativeSrc.write.json(pathOut + '/srcVertex/k8s-native')
    nativeDest.write.json(pathOut + '/destVertex/k8s-native')
    #nativeEdge.write.json(pathOut + '/edge/k8s-native')
    nativeEdge.union(nativeNsEdge).write.json(pathOut + '/edge/k8s-native')

    # we will add tag to generate desthash for k8s-external resources
    # we found a case where container and atomic collides in vid if we don't add tag info
    # for container, containerID = null, containerName = init-mysql
    # for atomic, val = init-mysql
    # they both produce a vid = '16856238-1ab0-5308-a142-3a01a9ec69f5'
    ## container
    print('create vertex and edge for container ...')

    container = spark.read.json(pathIn + '/container')
    # if containerID is not null, we view is as determined
    container2 = container.withColumn('desthash', udf_uuid5(concat_ws('/', 'tag', 'containerID', 'containerName')))\
                            .withColumn('isDetermined', col('containerID').isNotNull())\
                            .cache()

    containerDest = container2.select('containerID', 'containerName', 'tag', col('desthash').alias('vid'),\
                                        'isAtomic', 'isNative', 'isDetermined')\
                                .distinct()
    # for k8s external resources, we set edge type as UseExternal,
    # and use kind as srcKind, and tag as destKind
    containerEdge = container2.withColumn('type', lit('UseExternal'))\
                                .withColumn('srcKind', col('kind'))\
                                .withColumn('destKind', col('tag'))\
                                .select(col('metahash').alias('srcVid'), col('desthash').alias('destVid'),\
                                'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')\
                                .distinct()
    
    containerDest.write.json(pathOut + '/destVertex/container')
    containerEdge.write.json(pathOut + '/edge/container')

    ## image
    print('create vertex and edge for image ...')

    image = spark.read.json(pathIn + '/image')
    # imageID can not determine the image entity, we will use (imageID, imageName) pair
    image2 = image.withColumn('desthash', udf_uuid5(concat_ws('/', 'tag', 'imageID', 'imageName')))\
                    .withColumn('isDetermined', col('imageID').isNotNull() & col('imageName').isNotNull())\
                    .cache()

    imageDest = image2.select('imageID', 'imageName', 'tag', col('desthash').alias('vid'),\
                                'isAtomic', 'isNative', 'isDetermined')\
                        .distinct()
    
    imageEdge = image2.withColumn('type', concat(lit('UseExternal')))\
                        .withColumn('srcKind', col('kind'))\
                        .withColumn('destKind', col('tag'))\
                        .select(col('metahash').alias('srcVid'), col('desthash').alias('destVid'),\
                                'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')\
                        .distinct()

    imageDest.write.json(pathOut + '/destVertex/image')
    imageEdge.write.json(pathOut + '/edge/image')

    ## hostpath
    print('create vertex and edge for hostpath ...')

    hostpath = spark.read.json(pathIn + '/hostpath')
    hostpath2 = hostpath.withColumn('desthash', udf_uuid5(concat_ws('/', 'tag', 'path', 'server')))\
                        .withColumn('isDetermined', col('path').isNotNull() & col('server').isNotNull())\
                        .cache()

    hostpathDest = hostpath2.select('path', 'server', 'tag', col('desthash').alias('vid'),\
                                'isAtomic', 'isNative', 'isDetermined')\
                        .distinct()
    
    hostpathEdge = hostpath2.withColumn('type', lit('UseExternal'))\
                            .withColumn('srcKind', col('kind'))\
                            .withColumn('destKind', col('tag'))\
                            .select(col('metahash').alias('srcVid'), col('desthash').alias('destVid'),\
                                'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')\
                            .distinct() 

    hostpathDest.write.json(pathOut + '/destVertex/hostpath')
    hostpathEdge.write.json(pathOut + '/edge/hostpath')

    ## nfs
    # nfs and hostpath are similar
    print('create vertex and edge for nfs ...')

    nfs = spark.read.json(pathIn + '/nfs')
    nfs2 = nfs.withColumn('desthash', udf_uuid5(concat_ws('/', 'tag', 'path', 'server')))\
                .withColumn('isDetermined', col('path').isNotNull() & col('server').isNotNull())\
                .cache()

    nfsDest = nfs2.select('path', 'server', 'tag', col('desthash').alias('vid'),\
                        'isAtomic', 'isNative', 'isDetermined')\
                    .distinct()

    nfsEdge = nfs2.withColumn('type', lit('UseExternal'))\
                    .withColumn('srcKind', col('kind'))\
                    .withColumn('destKind', col('tag'))\
                    .select(col('metahash').alias('srcVid'), col('desthash').alias('destVid'),\
                                'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')\
                    .distinct()
    
    nfsDest.write.json(pathOut + '/destVertex/nfs')
    nfsEdge.write.json(pathOut + '/edge/nfs')

    ## atomic
    print('create vertex and edge for atomic ...')

    atomic = spark.read.json(pathIn + '/atomic')
    atomic2 = atomic.withColumn('desthash', udf_uuid5(concat_ws('/', 'tag', 'val')))\
                    .withColumn('isDetermined', col('val').isNotNull())\
                    .cache()
    
    atomicDest = atomic2.select('val', 'tag', col('desthash').alias('vid'),\
                                'isAtomic', 'isNative', 'isDetermined')\
                        .distinct()

    atomicEdge = atomic2.withColumn('type', lit('UseExternal'))\
                        .withColumn('srcKind', col('kind'))\
                        .withColumn('destKind', col('tag'))\
                        .select(col('metahash').alias('srcVid'), col('desthash').alias('destVid'),\
                                'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')\
                        .distinct()

    atomicDest.write.json(pathOut + '/destVertex/atomic')
    atomicEdge.write.json(pathOut + '/edge/atomic')

    spark.stop()


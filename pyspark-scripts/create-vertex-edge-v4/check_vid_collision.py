# check the possible vid collisions which are created from uuid5
# we will check within each resource and across all resources, i.e, container, image, nfs together


import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


# main function
if __name__ == "__main__":
    """
        Usage: check_vid_collision.py [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonCheckVidCollision")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]
    
    ##k8s-native
    nativeSrc = spark.read.json(pathIn + '/srcVertex/k8s-native')
    nativeDest = spark.read.json(pathIn + '/destVertex/k8s-native')
    # nativeSrc and nativeDest can share some vertices
    # distinct before select, otherwise we will miss the collision
    nativeVidTag = nativeSrc.union(nativeDest)\
                            .distinct()\
                            .withColumn('tag', lit('native'))\
                            .select('vid', 'tag')

    # container
    containerVidTag = spark.read.json(pathIn + '/destVertex/container')\
                            .select('vid', 'tag')
    
    # image
    imageVidTag = spark.read.json(pathIn + '/destVertex/image')\
                        .select('vid', 'tag')

    # hostpath
    hostpathVidTag = spark.read.json(pathIn + '/destVertex/hostpath')\
                            .select('vid', 'tag')

    # nfs
    nfsVidTag = spark.read.json(pathIn + '/destVertex/nfs')\
                        .select('vid', 'tag')

    # atomic
    atomicVidTag = spark.read.json(pathIn + '/destVertex/atomic')\
                        .select('vid', 'tag')

    # union together
    vidTag = nativeVidTag.union(containerVidTag)\
                        .union(imageVidTag)\
                        .union(hostpathVidTag)\
                        .union(nfsVidTag)\
                        .union(atomicVidTag)
    # check collsion
    # collect_list() will capture the collisions within each resource, i.e, two images share a vid
    vidCollision = vidTag.groupBy('vid')\
                        .agg(collect_list('tag').alias('tags'), count('tag').alias('count'))\
                        .filter(col('count') > 1)\
                        .cache()
    
    if (vidCollision.count() == 0):
        print('no collision found in the vid\n')
    else:
        print('the 10 example vid collisions are ...\n')
        vidCollision.show(10, False)

    vidCollision.write.json(pathOut)

    spark.stop()

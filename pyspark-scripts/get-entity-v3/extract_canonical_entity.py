## get k8s-native and k8s-external (i.e, container, image) entity
## and build the canonical identity which maps partial name/id to full identity
# container: [containerName, containerID]
# image: [imageName, imageID]
# hostPath/nfs: [path, server]
# atomic: val
# k8s-native: [uid, name, namespace, kind, selfLink]

import sys
import json, re
from pyspark.sql.functions import *
from pyspark.sql.types import * 
from pyspark.sql import SparkSession

from get_atomic import get_atomic
from get_nfs import get_nfs
from get_hostpath import get_hostpath
from get_image import get_image
from get_container import get_container

from get_k8s_native import get_k8s_native

# Function for flattening json
def flatten_json(y):
    out = {}

    def flatten(x, name =''):
        # If the nested key-value pair is of dict type
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        # If the nested key-value pair is of list type
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def refined_prefix(key):
    # key can be 'status_images_23_names_0', 'metadata_ownerReferences_0_name' or 'kind', 'timestamp'
    tmp = key.rsplit('_', 1)
    if tmp[-1].isdigit(): # get last word, don't use tmp[1] which may out-of-range
        return key
    else:
        return tmp[0]

# main function 
if __name__ == "__main__":
    """
        Usage: get_entity.py [rescPathIn] [tagPathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonGetEntity")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    rescPathIn = sys.argv[1]
    tagPathIn = sys.argv[2]
    pathOut = sys.argv[3]

    # flatten resource rows
    # note: metadata.uid is None in ComponentStatus
    rescFlatDF = spark.read.text(rescPathIn)\
                        .rdd.map(lambda x: json.loads(x.value))\
                        .map(lambda x: ((x['kind'], x['timestamp'], x['metahash'],\
                                        x['metadata']['uid'] if 'uid' in x['metadata'] else None), flatten_json(x)))\
                        .flatMapValues(lambda x: [(k,v) for k,v in x.items()])\
                        .mapValues(lambda x: ((re.sub('_\d+', '', x[0]), refined_prefix(x[0])), x[1]))\
                        .map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[1][0][0], x[1][0][1], x[1][1]))\
                        .toDF(['kind', 'timestamp', 'metahash', 'metadataUid', 'key', 'prefix', 'val'])\
                        .distinct()

    tagDF = spark.read.csv(tagPathIn, header=True)\
                .withColumn('isNative', col('isNative').cast(BooleanType()))\
                .withColumn('isAtomic', col('isAtomic').cast(BooleanType()))

    rescTagDF = rescFlatDF.join(tagDF, ['kind', 'key'], 'inner').cache()
    
    # get k8s-external container
    print('get k8s-external container ...') 
    containerResc = rescTagDF.filter((col('isNative') == False) & (col('tag') == 'container'))
    containerResult = get_container(containerResc)
    containerResult.write.json(pathOut + '/container')
    
    # get k8s-external image
    print('get k8s-external image ...')
    imageResc = rescTagDF.filter((col('isNative') == False) & (col('tag') == 'image')) 
    imageResult = get_image(imageResc)
    imageResult.write.json(pathOut + '/image')
   
    # get k8s-external nfs 
    print('get k8s-external nfs ...')
    nfsResc = rescTagDF.filter((col('isNative') == False) & (col('tag') == 'nfs')) 
    nfsResult = get_nfs(nfsResc) 
    nfsResult.write.json(pathOut + '/nfs')
    #nfsResult.show(5)
    
    ## get k8s-external hostpath
    print('get k8s-external hostpath ...')
    hostPathResc = rescTagDF.filter((col('isNative') == False) & (col('tag') == 'hostPath'))
    hostPathResult = get_hostpath(hostPathResc)
    hostPathResult.write.json(pathOut + '/hostpath')
   
    # get k8s-external atomic
    print('get k8s-external atomic ...')
    atomicResc = rescTagDF.filter((col('isNative') == False) & (col('isAtomic') == True))
    atomicResult = get_atomic(atomicResc)
    atomicResult.write.json(pathOut + '/atomic')

    # get k8s-native entity
    print('get k8s-native entity ...')
    nativeResc = rescTagDF.filter(col('isNative') == True)
    nativeResult = get_k8s_native(nativeResc)
    nativeResult.write.json(pathOut + '/k8s-native')

    spark.stop()

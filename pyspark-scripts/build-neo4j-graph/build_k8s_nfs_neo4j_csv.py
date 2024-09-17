# we convert both k8s-api-resource and nfs vertex+edge into csv for neo4j

# convert srcVertex, destVertex and edge into csv format for neo4j
# edge/*, srcVertex/k8s-native, destVertex/[container, image, hostpath, nfs, atomic] have only one level,
# and we can easily save as csv
# but, destVertex/k8s-api-resource contains many kind of resources, i.e, Pod, Job, Event; and they have 
# different fields. we don't use the union schema which introduces many nulls. we will read.text() then json.loads()
# and convert the json to csv which only keep the top level keys, and the sub-structures will be convert as a string
# note: the repartition(1) makes the running time quite long

# we can convert json to csv (neo4j uses it) immediately after create-vertex-edge,
# or after graphframe build-entitygraph (with partial reverse edges)

import sys
import json
#import ast
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# get header for edge and vertex
# note: don't add ':TYPE' to 'type' in vertex keys (i.e, Event, Secret)
# to tolerate the keys that change in graphframe, we use
# :ID for [vid, id], :START_ID for [srcVid, src], :END_ID for [destVid, dst]

def get_header(ks, delimiter=';', mode='vertex'):
    res = ''
    if mode == 'vertex':
        for x in ks:
            if x in ['vid', 'id']:
                x += ':ID'
            res = res + delimiter + x
    elif mode == 'edge':
        for x in ks:
            if x in ['srcVid', 'src']:
                x += ':START_ID'
            elif x in ['destVid', 'dst']:
                x += ':END_ID'
            elif x == 'type':
                x += ':TYPE'
            res = res + delimiter + x
    else:
        print('mode can only be vertex or edge')
        exit(1)

    return res[1:] # remove the leading delimiter

# fill dict with None if key is not exist, and use the ordering in keys
def dictfill(dct, keys):
    # we need a new dict to keep the ordering of keys
    dct2 = dict()
    for k in keys:
        dct2[k] = dct.get(k, None)
    return dct2

def dict2csv(dct, delimiter=';'):
    # the keys are in the same order by default
    # since we read from dataframe with same schema
    # so we omit the sorted(keys)
    res = ''
    for k,v in dct.items():
        # avoid delimiter, \n and \r in value (other escape sequences are not encountered)
        v2 = str(v).replace(delimiter, ' ')\
                    .replace('\n', ' ')\
                    .replace('\r', ' ')
        res = res + delimiter + v2
    return res[1:]



# main function
if __name__ == "__main__":
    """
        Usage: build_k8s_nfs_neo4j_csv.py [k8sPathIn] [nfsPathIn] [pathOut] [withReverse]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonBuildK8sNfsNeo4jCSV")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # k8sPathIn = testPath + '/k8s-api-resource-create-vertex-edge-v2'
    k8sPathIn = sys.argv[1]
    nfsPathIn = sys.argv[2]
    pathOut = sys.argv[3]
    withReverse = sys.argv[4]
    #withReverse = ast.literal_eval(sys.argv[4])
    #withReverse = bool(sys.argv[4])
    
    # edges share the same schema
    print('convert edge/* to csv ...')

    # cmdline is string
    if withReverse == 'True':
        print('include partial reversed edges')
        k8sEdgePathIn = k8sPathIn + '/edge-reverse-partial'
    else:
        k8sEdgePathIn = k8sPathIn + '/edge/*'

    edgePathIn = [k8sEdgePathIn, nfsPathIn + '/edge/*']

    # edges in k8s and nfs share the same schema
    edge = spark.read.json(edgePathIn)
    # we can write to the same directory
    edge.repartition(1).write.options(delimiter=';').csv(pathOut + '/edge/')
    # we treat edgeHeader and vertexHeader seperately 
    edgeHeader = get_header(edge.schema.names, mode='edge')
    spark.sparkContext.parallelize([edgeHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/edge/')

    # srcVertex only containes k8s-native  
    print('convert srcVertex/k8s-native to csv ...')
   
    # we put srcVertex/k8s-native and destVertex/k8s-native together
    native = spark.read.json([k8sPathIn + '/srcVertex/k8s-native/', k8sPathIn + '/destVertex/k8s-native/'])\
                    .distinct()\
                    .cache()
    #native = spark.read.json(k8sPathIn + '/srcVertex/k8s-native/').cache()
    #native.repartition(1).write.options(delimiter=';').csv(pathOut + '/srcVertex/k8s-native/')
    kind2s = native.select('kind2').distinct()\
                    .orderBy('kind2')\
                    .rdd.map(lambda x: x.kind2)\
                    .collect()
    # we will split native by kind2, therefore we can annotate them in neo4j                
    for kd2 in kind2s:
        print(kd2)
        native.filter(col('kind2') == kd2)\
                .repartition(1)\
                .write.options(delimiter=';')\
                .csv(pathOut + '/srcVertex/k8s-native/' + kd2)
                #.csv(pathOut + '/srcVertex/' + kd2)
    
    # they share the same header
    nativeHeader = get_header(native.schema.names)
    spark.sparkContext.parallelize([nativeHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/srcVertex/k8s-native/')
            #.saveAsTextFile(pathOut + '/header/srcVertex/')

    # container, image, hostpath, atmoic in destVertex
    print('convert destVertex/[container, image, hostpath, atomic] to csv ...')

    for ext in ['container', 'image', 'hostpath', 'atomic']:
        external = spark.read.json(k8sPathIn + '/destVertex/' + ext)
        external.repartition(1).write.options(delimiter=';').csv(pathOut + '/destVertex/' + ext)
        externalHeader = get_header(external.schema.names)
        spark.sparkContext.parallelize([externalHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/destVertex/' + ext)
    
    # nfs in k8sPathIn/destVertex and nfsPathIn/srcVertex
    print('special case: convert nfs from k8s and nfs to csv ...')
    # read and distinct
    nfs = spark.read.json([k8sPathIn + '/destVertex/nfs', nfsPathIn + '/srcVertex/nfs'])\
                .distinct()
    nfs.repartition(1).write.options(delimiter=';').csv(pathOut + '/destVertex/nfs')
    # create nfs-header
    nfsHeader = get_header(nfs.schema.names)
    spark.sparkContext.parallelize([nfsHeader])\
        .repartition(1)\
        .saveAsTextFile(pathOut + '/header/destVertex/nfs')

    
    # k8s-api-resource in destVertex
    # we will deal with each kind, and only save the non-null field
    # we read.text() then json.loads(), and convert top level key-val pairs
    print('convert all kinds in destVertex/k8s-api-resource to csv ...')
    

    print('convert destVertex/nfs to csv ....')
    
    print('we put NFS together with POD, JOB, etc')
    # cache is required
    #resource = spark.read.text(k8sPathIn + '/destVertex/k8s-api-resource')\
    resource = spark.read.text([k8sPathIn + '/destVertex/k8s-api-resource', nfsPathIn + '/destVertex/NFS'])\
                    .rdd.map(lambda x: json.loads(x.value))\
                    .cache()

    kinds = resource.map(lambda x: x['kind']).distinct()\
                    .sortBy(lambda x: x).collect()

    kindKeys = resource.map(lambda x: (x['kind'], set(x.keys())))\
                    .reduceByKey(lambda x, y: x.union(y))\
                    .mapValues(lambda x: sorted(list(x)))\
                    .collectAsMap()

    # note: resource are actually xxSnapshot, i.e, Pod -> PodSnapshot
    # we save it as <kd>-upper, and we will use upper case in neo4j
    for kd in kinds:
        print(kd.upper())
        # get keys for each kind, i.e, Pod, Job
        names = kindKeys[kd]
        resc = resource.filter(lambda x: x['kind'] == kd)
        # we must fill the dict with None and keep the same ordering as names
        resc.map(lambda x: dictfill(x, names))\
            .map(lambda x: dict2csv(x))\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/destVertex/k8s-nfs-snapshot/' + kd.upper())
            #.saveAsTextFile(pathOut + '/destVertex/k8s-api-resource/' + kd.upper())
            #.saveAsTextFile(pathOut + '/destVertex/' + kd.upper())
            #.saveAsTextFile(pathOut + '/destVertex/k8s-api-resource/' + kd + 'Snapshot')
        
        #bug: the keys for each dict is not the same, because there are None in value, and the key disappears   
        #names = list(resc.take(1)[0].keys()) 
        rescHeader = get_header(names)  
        spark.sparkContext.parallelize([rescHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/destVertex/k8s-nfs-snapshot/' + kd.upper())
            #.saveAsTextFile(pathOut + '/header/destVertex/k8s-api-resource/' + kd.upper())
            #.saveAsTextFile(pathOut + '/header/destVertex/' + kd.upper())
            #.saveAsTextFile(pathOut + '/header/destVertex/k8s-api-resource/' + kd + 'Snapshot')
    
    spark.stop()

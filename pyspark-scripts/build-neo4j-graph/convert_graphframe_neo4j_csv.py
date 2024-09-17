# convert graphframe (in spark) to neo4j in csv format
# pathIn: graphframePathIn
# pathOut: neo4jPathOut


import sys
import json
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


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

def drop_null_columns(df):
    # count the df length once
    df_length = df.count()
    # count nulls in each column, we can alias(c+'Cnt') for debug
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])\
                    .collect()[0].asDict()
    to_drop = [k for k, v in null_counts.items() if v >= df_length]
    df = df.drop(*to_drop)
    return df


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
        Usage: convert_graphframe_neo4j_csv.py [graphframePathIn] [neo4jPathOut] [fixed]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonConvertGraphframeNeo4j")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    pathIn = sys.argv[1]
    pathOut = sys.argv[2]
    fixed = sys.argv[3]

    #### edges share the same schema
    print('convert edge/* to csv ...')
    if fixed.lower() == 'fixed':
        edge = spark.read.json(pathIn + '/fixed-edges')
    else:    
        edge = spark.read.json(pathIn + '/edges')
    
    edge.repartition(1).write.options(delimiter=';').csv(pathOut + '/edge/')
    
    # get header for edges 
    edgeHeader = get_header(edge.schema.names, mode='edge')
    spark.sparkContext.parallelize([edgeHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/edge/')
  
   
    
    #### vertex: entity and snapshot (both k8s-api-resource and NFS, with different keys)
    # entity: native and external (atomic, container, image, hostpath, nfs)
    entity = spark.read.json(pathIn + '/vertices')\
                    .filter(col('kind').isNull())\
                    .cache()

    ### k8s-native entity
    print('convert k8s-native entity to csv ...')

    native = drop_null_columns(entity.filter(col('isNative') == True)).cache()
    # we will split native by kind2, therefore we can annotate them in neo4j  
    kind2s = native.select('kind2').distinct()\
                    .orderBy('kind2')\
                    .rdd.map(lambda x: x.kind2)\
                    .collect()

    for kd2 in kind2s:
        print(kd2)
        native.filter(col('kind2') == kd2)\
                .repartition(1)\
                .write.options(delimiter=';')\
                .csv(pathOut + '/srcVertex/k8s-native/' + kd2)

    # share the same header
    nativeHeader = get_header(native.schema.names)
    spark.sparkContext.parallelize([nativeHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/srcVertex/k8s-native/')

    ### k8s-external entity: container, image, hostpath, nfs, atmoic in destVertex
    print('convert k8s-external entity to csv ...')
    
    external = entity.filter(col('isNative') == False)

    ## atomic use (tag, val), but tag can be hostname, volumeName, handleID
    tag = 'atomic'
    print(tag)
    atomic = drop_null_columns(external.filter(col('isAtomic') == True))
    atomic.repartition(1)\
            .write.options(delimiter=';')\
            .csv(pathOut + '/destVertex/' + tag)  

    atomicHeader = get_header(atomic.schema.names)
    spark.sparkContext.parallelize([atomicHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/destVertex/' + tag)

    ## nonAtomic include container, image, hostpath and nfs
    tags = external.filter(col('isAtomic') == False)\
                    .select('tag').distinct()\
                    .orderBy('tag')\
                    .rdd.map(lambda x: x.tag)\
                    .collect()
    
    for tag in tags:
        print(tag)
        ext = drop_null_columns(external.filter(col('tag') == tag))
        # write out
        ext.repartition(1)\
            .write.options(delimiter=';')\
            .csv(pathOut + '/destVertex/' + tag)

        # different headers
        extHeader = get_header(ext.schema.names)
        spark.sparkContext.parallelize([extHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/destVertex/' + tag)

    ## snapshot: k8s-api-resource (i.e, POD) and nfs snapshot (NFS)
    # snapshot has multi-level structure, we can not convert it to csv in dataframe,
    # so we read snapshot as text and convert it to json
    # actually, we can use the following method to convert top-level keys, but there are 'null's in deeper levels
    # table.select([col(c).cast("string") for c in table.columns])

    print('convert snapshot to csv ...')

    snapshot = spark.read.text(pathIn + '/vertices')\
                    .rdd.map(lambda x: json.loads(x.value))\
                    .filter(lambda x: 'kind' in x)\
                    .cache()

    kinds = snapshot.map(lambda x: x['kind']).distinct()\
                    .sortBy(lambda x: x).collect()

    kindKeys = snapshot.map(lambda x: (x['kind'], set(x.keys())))\
                        .reduceByKey(lambda x, y: x.union(y))\
                        .mapValues(lambda x: sorted(list(x)))\
                        .collectAsMap()

    for kd in kinds:
        print(kd.upper())
        # get keys for each kind, i.e, Pod, Job
        names = kindKeys[kd]
        snap = snapshot.filter(lambda x: x['kind'] == kd)
        
        # we must fill the dict with None and keep the same ordering as names
        snap.map(lambda x: dictfill(x, names))\
            .map(lambda x: dict2csv(x))\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/destVertex/k8s-nfs-snapshot/' + kd.upper())
        
        # create header for each kind
        snapHeader = get_header(names)
        spark.sparkContext.parallelize([snapHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/destVertex/k8s-nfs-snapshot/' + kd.upper())


    '''
    #  the 'kind' field is not NULL
    snapshot = vertex.filter(col('kind').isNotNull()).cache() 
    
    kinds = snapshot.select('kind').distinct()\
                    .orderBy('kind')\
                    .rdd.map(lambda x: x.kind)\
                    .collect()
    
    # can not write struct, we only keep the top-level (TODO) 
    for kd in kinds:
        print(kd.upper())
        snap = drop_null_columns(snapshot.filter(col('kind') == kd))
        
        # we can convert the top-level to string, but the deeper levels still contains many nulls
        snap2 = snap.select([col(c).cast("string") for c in  snap.schema.names])

        snap2.repartition(1)\
            .write.options(delimiter=';')\
            .csv(pathOut + '/destVertex/k8s-nfs-snapshot/' + kd.upper())

        snapHeader = get_header(snap.schema.names)
        spark.sparkContext.parallelize([snapHeader])\
            .repartition(1)\
            .saveAsTextFile(pathOut + '/header/destVertex/k8s-nfs-snapshot/' + kd.upper())
    '''

    spark.stop()

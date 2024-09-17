# build entitygraph and metagraph

import sys
from graphframes import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window



# main function
if __name__ == "__main__":
    """
        Usage: build_graph_metagraph.py [k8sPathIn] [nfsPathIn] [graphPathOut] [metagraphPathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonBuildGraphMetagraph")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    k8sPathIn = sys.argv[1]
    nfsPathIn = sys.argv[2]
    graphPathOut = sys.argv[3]
    metagraphPathOut = sys.argv[4]

    # vertex and edge pathIn
    vertexPath = []
    edgePath = []
    for i in [k8sPathIn, nfsPathIn]:
        for j in ['/srcVertex/*', '/destVertex/*']:
            vertexPath.append(i + j)
        edgePath.append(i + '/edge/*')

    print('build entitygraph ...')
        
    # there can be duplicated vertices, i,e. Pod->nfs, nfs->NFS 
    # but edges are not duplicated
    vertex = spark.read.json(vertexPath).distinct()
    edge = spark.read.json(edgePath)

    # rename vertex and edge
    vertex2 = vertex.withColumnRenamed('vid', 'id')
    edge2 = edge.withColumnRenamed('srcVid', 'src').withColumnRenamed('destVid', 'dst')
    
    '''
    # build partial undirected graph, only add reversed edge for one-to-one mapping
    tt = edge2.groupBy('dst', 'destKind', 'key', 'srcKind', 'type').count().drop('dst').distinct()
    t1 = tt.filter(col('count') == 1)
    t2 = tt.filter(col('count') > 1)
    t3 = t1.drop('count').subtract(t2.drop('count'))
    # we can choose type from [ReferInteral, UseExternal, HasState, HasEvent] to add reverse edge
    # currently, we build only for ReferInternal 
    t4 = t3.filter(col('type') == 'ReferInternal')

    # build reverse edge
    edge3 = edge2.join(t4, ['destKind', 'key', 'srcKind', 'type'],'inner')\
                .select(col('srcKind').alias('destKind'), col('src').alias('dst'), 'key',\
                        col('destKind').alias('srcKind'), col('dst').alias('src'), 'tmax', 'tmin', 'type')\
                .withColumn('isReverse', lit(True))

    edge4 = edge2.withColumn('isReverse', lit(False)).union(edge3)
    edge4.cache()
    '''
    # we don't add the reversed edge, i.e, Pod->Job->CronJob with metadata_ownerReferences_uid, 
    # but we don't like Job->Pod->ReplicaSet/StatefulSet/DaemonSet
    edge4 = edge2.cache()

    # build graph and cache it
    entityGraph = GraphFrame(vertex2, edge4)

    print('build metagraph ...')

    # build metagraph with category for vertex, i.e, NativeEntity, ExteralEntity, Snapshot 
    # add count and category 
    # HasState: Pod->POD, nfs->NFS
    # HasEvent: Event->EVENT
    # ReferInternal: Event->Pod
    # UseExternal: Pod->nfs
    #meta = edge4.groupBy('srcKind', 'destKind', 'key', 'type', 'isReverse').count()
    # if we don't add reverse edge, leave the isReverse field
    meta = edge4.groupBy('srcKind', 'destKind', 'key', 'type').count()
    meta2 = meta.withColumn('src', hash('srcKind')).withColumn('dst', hash('destKind'))\
                .withColumn('srcCategory', when(col('srcKind') == 'nfs', lit('ExternalEntity'))\
                                            .otherwise(lit('NativeEntity')))\
                .withColumn('destCategory', when(col('type') == 'ReferInternal', 'NativeEntity')\
                                        .when(col('type') == 'UseExternal', 'ExternalEntity')\
                                        .when(col('type').isin(['HasState', 'HasEvent']), 'Snapshot')\
                                        .otherwise('Unknown'))

    # build meta vertex
    metaSrc = meta2.select(col('src').alias('id'), col('srcKind').alias('kind'), col('srcCategory').alias('category'))\
                    .distinct()
    metaDst = meta2.select(col('dst').alias('id'), col('destKind').alias('kind'), col('destCategory').alias('category'))\
                    .distinct()
    metaVertex = metaSrc.union(metaDst).distinct()

    # build meta edge
    metaEdge = meta2.drop('srcCategory', 'destCategory')

    # build metaGraph
    metaGraph = GraphFrame(metaVertex, metaEdge)

    print('write entitygraph to hdfs ...')

    # write entitygraph
    entityGraph.vertices.write.json(graphPathOut + '/vertices')
    entityGraph.edges.write.json(graphPathOut + '/edges')
    
    '''
    # we can also only save the edges, since the vertices are not changed
    entityGraph.edges.withColumnRenamed('dst', 'destVid')\
                .withColumnRenamed('src', 'srcVid')\
                .write.json(k8sPathIn + '/edge-reverse-partial')
    '''

    print('write metagraph to hdfs ...')
    # write metagraph
    metaGraph.vertices.write.json(metagraphPathOut + '/vertices')
    metaGraph.edges.write.json(metagraphPathOut + '/edges')
    
    spark.stop()

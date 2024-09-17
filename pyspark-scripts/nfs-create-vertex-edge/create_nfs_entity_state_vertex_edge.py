# create (nfs)-[HasState]->(NFS) relations
# vertex: nfs (more entities than nfs found in k8s-api-resource), NFS
# edge: HasState
# pathIn: first and last nfs snapshots
# pathOut: nfs, NFS, HasState

import sys
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


udf_uuid5 = udf(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)), StringType())

# main function
if __name__ == "__main__":
    """
        Usage: create_nfs_entity_state_vertex_edge.py [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonCreateNfsVertexEdge")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]
    
    # use metahash+timestamp as the vid for NFS
    print('create entity+state vertex and edge for nfs ...')
   
    # we use [timestamp, nextTimestamp] to indicate the time-range of snapshot (i,e. PodSnapshot or POD)   
    # and remove the last snapshot to reduce data size and also keep last two snapshots distinct
    # Note: if windowSize is 1, we can not directly use lead('timestamp')
    w1 = Window.partitionBy('metahash').rowsBetween(-sys.maxsize,sys.maxsize)
    w2 = Window.partitionBy('metahash').orderBy('timestamp')
    
    nfs = spark.read.json(pathIn)\
                .withColumn('windowSize', count('metahash').over(w1))\
                .withColumn('nextTimestamp', when(col('windowSize') > 1, lead('timestamp').over(w2))\
                                            .otherwise(col('timestamp')))
                
    nfs2 = nfs.filter(col('nextTimestamp').isNotNull())\
                    .withColumn('vid', udf_uuid5(concat_ws('/', 'metahash', 'timestamp')))\
                    .cache()

    # create nfsEntity and nfsState vertex
    # use 'tag' in nfsEntity
    nfsEntity = nfs2.select('path', 'server', col('kind').alias('tag'), col('metahash').alias('vid'))\
                    .withColumn('isAtomic', lit(False))\
                    .withColumn('isDetermined', lit(True))\
                    .withColumn('isNative', lit(False))
   
    #nfsState = nfs2.drop('windowSize', 'nextTimestamp')
    # we keep timestamp and nextTimestamp as tmin/tmax
    nfsState = nfs2.drop('windowSize')

    # we create nfs-[HasState]->NFS edge
    # kind in nfsState is 'nfs'
    nfsEdge = nfs2.select(col('metahash').alias('srcVid'), 'kind', col('timestamp').alias('tmin'),\
                            col('nextTimestamp').alias('tmax'), col('vid').alias('destVid'))\
                            .withColumn('type', lit('HasState'))\
                            .withColumn('srcKind', col('kind'))\
                            .withColumn('destKind', upper(col('kind')))\
                            .withColumn('key', lit('path'))\
                            .select('srcVid', 'destVid', 'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')
    
    # write vertex and edge
    nfsEntity.write.json(pathOut + '/srcVertex/nfs')
    nfsState.write.json(pathOut + '/destVertex/NFS')
    nfsEdge.write.json(pathOut + '/edge/NFS')

    spark.stop()

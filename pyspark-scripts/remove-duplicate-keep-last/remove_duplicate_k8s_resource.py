'''
Remove consecutive duplicates in k8s api resources, 
and keep the last snapshot (maybe a duplicate) to determine the life-cycle of k8s-entity
'''

import sys
import uuid
from pyspark.sql.functions import * 
from pyspark.sql.functions import hash as hash2
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: remove_duplicate_k8s_resource [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonRemoveDuplicateK8sResource")\
        .getOrCreate()

    # set log level to 'WARN' 
    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]

    # add the directory info, then explode and expand, then distinct within file 
    rescDF = spark.read.option('multiline', 'true')\
                .json(pathIn + '/all_resources.json')\
                .withColumn('path', input_file_name())\
                .select(explode(col('items')), 'path')\
                .select(col('col.*'), 'path')\
                .distinct()\
                .withColumn('path', regexp_extract(col('path'), '^.*/', 0)) # remove filename, keep directory

    # aux info, i.e, timestamp 
    auxDF = spark.read.option('multiline', 'true')\
                .json(pathIn + '/aux_node_timestamp.json')\
                .withColumn('path', input_file_name())\
                .withColumn('path', regexp_extract(col('path'), '^.*/', 0))

    # join by 'path', under the same directory            
    joinDF = rescDF.join(auxDF, 'path', 'inner')

    # use hash([uid, name, namespace, kind]) as the unique metahash
    # rename F.hash() as hash2, avoid mistakes with python native hash()
    #joinDFx = joinDF.withColumn('metahash', hash2(array('metadata.uid', 'metadata.name', 'metadata.namespace', 'kind')))
    
    # concat_ws() with '/' will produce less collisions
    # bug: hash2() can not avoid collision
    #joinDFx = joinDF.withColumn('metahash', hash2(concat_ws('/', 'metadata.uid', 'metadata.name', 'metadata.namespace', 'kind')))

    udf_uuid5 = udf(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)), StringType())
    # we don't generate uuid5 for all rows, which may introduce more collisions 
    #joinDFx = joinDF.withColumn('metahash', udf_uuid5(concat_ws('/', 'metadata.uid', 'metadata.name', 'metadata.namespace', 'kind')))

    # ComponentStatus has null-uid
    # we prefer not generate metahash for all rows, and only for null-uid rows
    # (1) we may run into collisions if we generate x10 millions uuids, for all rows
    # (2) if we only generate uuid for ComponentStatus (actually, only 3 distinct uuids), the prob of collision is smaller
    # (3) in the worst case, if the uuid5 for ComponentStatus collide (with themselves or with other metadata.uid), we don't care
    # (4) the distinction of metadata.uid of non-null rows will be kept, and we will not collide them with metahash
    # (5) the metahash and metadata.uid will have a connection, and better for later use
    # (6) we found that (name, namespace, kind) can not uniquely identify an object, they may be reused in disjointed time-ranges
    #     so, don't directly generate uuid for all rows with these fields
    joinDFx = joinDF.withColumn('metahash', when(col('metadata.uid').isNotNull(), col('metadata.uid'))\
                                    .otherwise(udf_uuid5(concat_ws('/', 'metadata.name', 'metadata.namespace', 'kind'))))

    # remove consective duplicates with Window and null_safe function
    '''
    def null_safe_equality(a, b):
        return a == b

    udf_null_safe_equality = udf(null_safe_equality, BooleanType())

    w3 = Window().partitionBy('kind').orderBy(['metahash', 'timestamp'])
    
    joinDFx2 = joinDFx.withColumn('dupe', (col('metahash') == lag('metahash').over(w3))\
                                        & (udf_null_safe_equality(col('status'), lag('status').over(w3))))
    '''
    # use the build-in function eqNullSafe()
    # window can not cache()
    # dupe for removing duplicates, withNext for last snapshot (we use 'withNext' to avoid collison with 'hasNext')
    w3 = Window().partitionBy('kind').orderBy(['metahash', 'timestamp'])
    joinDFx2 = joinDFx.withColumn('dupe', (col('metahash') == lag('metahash').over(w3))\
                                        & (col('status').eqNullSafe(lag('status').over(w3))))\
                    .withColumn('withNext', (col('metahash') == lead('metahash').over(w3))\
                                        & (col('timestamp') <= lead('timestamp').over(w3)))
    
    joinDFx3 = joinDFx2.filter((col("dupe") == False) | (col("dupe").isNull())\
                            | (col('withNext') == False) | (col('withNext').isNull()))
   
    joinDFx3.write.json(pathOut)
    
    spark.stop()

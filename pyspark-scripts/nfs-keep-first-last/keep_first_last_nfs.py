'''
# we found that NFS directory does not change except the ctime
# so we can only keep the first and last snapshot to indicate the life-range of a persistent disk
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
        Usage: keep_first_last_nfs [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonKeepFirstLastNFS")\
        .getOrCreate()

    # set log level to 'WARN' 
    spark.sparkContext.setLogLevel("WARN")

    # /user/k8s/data-collection-20201208/172.16.112.85/storage-state/persistent-disk/2020-12-14_19.00.02.014/persistent_disks_out.json
    pathIn = sys.argv[1]
    pathOut = sys.argv[2]

    # add the directory info, then explode and expand, then distinct within file 
    nfsDF = spark.read.option('multiline', 'true')\
                .json(pathIn + '/persistent_disks_out.json')\
                .withColumn('hdfspath', input_file_name())\
                .distinct()\
                .withColumn('hdfspath', regexp_extract(col('hdfspath'), '^.*/', 0)) # remove filename, keep directory

    # aux info, i.e, timestamp 
    nfsAuxDF = spark.read.option('multiline', 'true')\
                .json(pathIn + '/aux_node_timestamp.json')\
                .withColumn('hdfspath', input_file_name())\
                .withColumn('hdfspath', regexp_extract(col('hdfspath'), '^.*/', 0))

    # join by 'hdfspath', under the same directory

    nfsJoinDF = nfsDF.join(nfsAuxDF, 'hdfspath', 'inner')
    
    # we confirmed that 172.16.112.85 is storage-dev.foobar.local and add it as 'server' 
    # replace with foobar
    udf_uuid5 = udf(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)), StringType())

    # similar to PodSnapshot, we use 'tag' to indicate the central object, not the snapshot
    # we use 'kind' for both k8s-api-resource and k8s-external
    # therefore, we can find snapshot with 'kind'
    # we confirmed, nfs is not in k8s-api-resource 

    nfsJoinDF2= nfsJoinDF.withColumn('server', lit('storage-dev.foobar.local'))\
                        .withColumn('kind', lit('nfs'))\
                        .withColumnRenamed('name', 'path')\
                        .withColumn('metahash', udf_uuid5(concat_ws('/', 'kind', 'path', 'server')))\


    # find the first and last snapshot with window function
    wd = Window().partitionBy('path').orderBy('timestamp')

    nfsJoinDFx = nfsJoinDF2.withColumn('withPrev', col('timestamp') >= lag('timestamp').over(wd))\
                        .withColumn('withNext', col('timestamp') <= lead('timestamp').over(wd))

    
    nfsJoinDFx2 = nfsJoinDFx.filter(col('withPrev').isNull() | col('withNext').isNull())

    nfsJoinDFx2.write.json(pathOut)

    spark.stop()


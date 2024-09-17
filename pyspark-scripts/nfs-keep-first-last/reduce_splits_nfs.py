'''
Reduce the output splits by keep_first_last_nfs, execute a second pass 
to eliminate any potential duplicates that may occur at split boundaries.
i.e, 2023-05 and 2023-06

'''

import sys
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: reduce_splits_nfs [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonReduceSplitsNFS")\
        .getOrCreate()

    # set log level to 'WARN' 
    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]

    # read in the splits and drop tag fields
    nfs = spark.read.json(pathIn)\
                .drop('withPrev', 'withNext')
    
    # window partition by kind
    wd = Window().partitionBy('path').orderBy('timestamp')

    nfs2 = nfs.withColumn('withPrev', col('timestamp') >= lag('timestamp').over(wd))\
                        .withColumn('withNext', col('timestamp') <= lead('timestamp').over(wd))
    
    # since we only need the min(timestamp) and max(timestamp),
    # we don't need use 'dupe' as that in k8s-api-resource
    nfs3 = nfs2.filter(col('withPrev').isNull() | col('withNext').isNull())

    nfs3.write.json(pathOut)
    
    spark.stop()

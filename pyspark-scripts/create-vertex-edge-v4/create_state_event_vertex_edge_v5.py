# create hasXXState and hasEvent relations for k8s api resource 
# we view the metadata entity as central, and build edge from metaEntity to State or Event
# pathIn is the k8s-api-resource with duplicates removed except the last snapshot
# pathOut will share with create_vertex_edge.py
# the srcVertex is shared by both scripts, we don't write the k8s-native again
# we simplify edge as HasState and HasEvent

import sys
import uuid
import re
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

from pyspark.sql.window import Window

udf_uuid5 = udf(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)), StringType())

@udf(returnType=StringType())
def replace_word_contains_special_char(text):
    chars = ['-', '_', '=', '~', '/', '?']
    words = re.split('[\n\s]', text)
    res = ''
    for word in words:
        for char in chars:
            if char in word:
                word = '$'
        res += word + ' '
    return res


# main function
if __name__ == "__main__":
    """
        Usage: create_state_event_vertex_edge.py [pathIn] [pathOut]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonCreateStateEventVertexEdge")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]
    
    # use metahash+timestamp as the vid for k8s-api-resource
    print('create state-or-event vertex and edge for k8s-api-resource ...')
   
    # we use [timestamp, nextTimestamp] to indicate the time-range of snapshot (i,e. PodSnapshot or POD)   
    # and remove the last snapshot to reduce data size and also keep last two snapshots distinct
    # Note: if windowSize is 1, we can not directly use lead('timestamp')
    w1 = Window.partitionBy('metahash').rowsBetween(-sys.maxsize,sys.maxsize)
    # We further consider resourceVersion
    #w2 = Window.partitionBy('metahash').orderBy('timestamp')
    w2 = Window.partitionBy('metahash').orderBy('metadata.resourceVersion', 'timestamp')

    resc = spark.read.json(pathIn)\
                .withColumn('windowSize', count('metahash').over(w1))\
                .withColumn('nextTimestamp', when(col('windowSize') > 1, lead('timestamp').over(w2))\
                                            .otherwise(col('timestamp')))
                
    # hostpath and nfs use [path, server] as keys, and the 'path' in resource is the input file path in hdfs
    # metahash+timestamp can not be unique, we further include resourceVersion
    '''
    resource = resc.filter(col('nextTimestamp').isNotNull())\
                    .withColumn('vid', udf_uuid5(concat_ws('/', 'metahash', 'timestamp')))\
                    .withColumnRenamed('path', 'hdfspath')\
                    .cache()
    '''
    resource = resc.filter(col('nextTimestamp').isNotNull())\
                    .withColumn('vid', udf_uuid5(concat_ws('/', 'metahash', 'metadata.resourceVersion', 'timestamp')))\
                    .withColumnRenamed('path', 'hdfspath')\
                    .cache()

    # add message template to Event, without build new vertex and edge
    outer_parentheses_pattern = r'\([^()]*\((?:[^()]*\([^()]*\))*[^()]*\)[^()]*\)'
    event = resource.filter(col('kind') == 'Event')\
                    .withColumn('msg', regexp_replace('message', '\(combined from similar events\): ', ''))\
                    .withColumn('msg', regexp_replace('msg', outer_parentheses_pattern, ''))\
                    .withColumn('msg', regexp_replace('msg', '\((.*?)\)', ''))\
                    .withColumn('msg', regexp_replace('msg', '\[(.*?)\]', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\{(.*?)\}', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\\\\"(.*?)\\\\"', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\"(.*?)\"', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\b\'(.*?)\'\b', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2}(?:\.\d{3})?)?', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '[a-fA-F0-9]{64}', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\d*\.\d+', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '(\$,)+', '\$,'))\
                    .withColumn('msg', replace_word_contains_special_char('msg'))\
                    .withColumn('msg', regexp_replace('msg', '\s\d+', ' \$ '))\
                    .withColumn('msg', regexp_replace('msg', '\scali.*\s', ' \$ '))\
                    .withColumn('msg', regexp_replace('msg', 'victim process: [\w,]+', 'victim process: \$'))\
                    .withColumn('msg', regexp_replace('msg', 'project \w+', 'project \$'))\
                    .withColumn('msg', regexp_replace('msg', 'StatefulSet \w+', 'StatefulSet \$'))\
                    .withColumn('msg', when(col('reason').isin(['Created', 'Started', 'Killing', 'Evicted']),\
                                                regexp_replace('msg', 'ontainer \w+', 'ontainer \$'))\
                                        .otherwise(col('msg')))\
                    .withColumn('msg', regexp_replace('msg', '\$ (Ki|Mi|Gi)', '\$ '))\
                    .withColumn('msg', trim('msg'))
    '''
    # for small dataset
    event = resource.filter(col('kind') == 'Event')\
                    .withColumn('msg', regexp_replace('message', '\(combined from similar events\): ', ''))\
                    .withColumn('msg', regexp_replace('msg', '\((.*?)\)', ''))\
                    .withColumn('msg', regexp_replace('msg', '\[(.*?)\]', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\"(.*?)\"', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\'(.*?)\'', '\$'))\
                    .withColumn('msg', regexp_replace('msg', '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,4}', '\$'))\
                    .withColumn('msg', replace_word_contains_special_char('msg'))\
                    .withColumn('msg', regexp_replace('msg', '\s\d+', ' \$ '))\
                    .withColumn('msg', regexp_replace('msg', '\scali.*\s', ' \$ '))\
                    .withColumn('msg', when(col('reason').isin(['Created', 'Started', 'Killing', 'Evicted']),\
                                                regexp_replace('msg', 'ontainer \w+', 'ontainer \$'))\
                                        .otherwise(col('msg')))\
                    .withColumn('msg', trim('msg'))
    '''

    nonEvent = resource.filter(col('kind') != 'Event').withColumn('msg', lit(None))
    # we don't include the helper fields 
    resource2 = event.union(nonEvent)
    
    # we view the metadata as central entity/object, and the kind indicates its category, i.e, Pod, Job
    # we take the api-resource snapshot as xxSnapshot
    # the full type info of an edge will be Pod-HasState-PodSnapshot, or Event-hasEvent-EventSnapshot
    # we will use upper-case (i.e, POD ~ PodSnapshot) to represent the snapshot
    # we may also set destKind as null, then it will be Pod-HasState, Event-HasEvent
    resourceEdge = resource.select(col('metahash').alias('srcVid'), 'kind', col('timestamp').alias('tmin'),\
                                    col('nextTimestamp').alias('tmax'), col('vid').alias('destVid'))\
                            .withColumn('type', when((col('kind') == 'Event'), lit('HasEvent'))\
                                                .otherwise(lit('HasState')))\
                            .withColumn('srcKind', col('kind'))\
                            .withColumn('destKind', upper(col('kind')))\
                            .withColumn('key', lit('metadata_uid'))\
                            .select('srcVid', 'destVid', 'type', 'srcKind', 'destKind', 'key', 'tmin', 'tmax')
    
    # write destVertex and edge
    # note: we have created srcVertex (k8s-native) in create_vertex_edge.py
    #resource.write.json(pathOut + '/destVertex/k8s-api-resource')
    #resource2.drop('windowSize', 'nextTimestamp').write.json(pathOut + '/destVertex/k8s-api-resource')
    
    # we keep timestamp and nextTimestamp as tmin/tmax, and not introduce extra tmin/tmax field
    resource2.drop('windowSize').write.json(pathOut + '/destVertex/k8s-api-resource') #mode='overwrite'

    resourceEdge.write.json(pathOut + '/edge/k8s-api-resource') #mode='overwrite'

    spark.stop()

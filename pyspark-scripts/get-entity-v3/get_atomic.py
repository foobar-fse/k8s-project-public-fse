# get atomic entity from multiple kinds and keys
# the atomic entities are all k8s external entities

from pyspark.sql.functions import *

#atomicTag = spark.read.csv(testPath + '/k8s-external-atomic.csv', header=True)
#atomicResc = rescFlatDF3.join(atomicTag, ['kind', 'key'], 'inner')

def get_atomic(atomicResc):
    # handle status.addresses.address in Node, which can be IP or hostname
    #address =  atomicResc.filter((col('kind') == 'Node') & (col('key') == 'status_addresses_address'))
    address = atomicResc.filter(col('tag') == 'address')
    # use 'ip' instead of 'IP' for external resource
    address2 = address.withColumn('tag', when(col('val').rlike('^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'), 'ip')\
                                            .otherwise('hostname'))
    
    atomicResult = atomicResc.subtract(address).union(address2)
    
    return atomicResult

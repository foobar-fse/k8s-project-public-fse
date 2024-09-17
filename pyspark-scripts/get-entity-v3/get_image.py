# get image entity from k8s api resource
# note: we only keep a single key for [imageName, imageID] pair
# i.e, [status_containerStatuses_image, status_containerStatuses_imageID] only keep the former key

#import copy
from pyspark.sql.functions import *


#imageTag = spark.read.csv(testPath + '/image.csv', header=True)
#imageResc = rescFlatDF3.join(imageTag, ['kind', 'key'], 'inner')


def get_image(imageResc):
    # filter out imageName and imageID
    imageName = imageResc.filter(col('tag2') == 'image').withColumnRenamed('val', 'imageName')
    imageID = imageResc.filter(col('tag2') == 'imageID').withColumnRenamed('val', 'imageID')

    # only care about 4 fields, no distinct needed
    tmpName = imageName.select('metadataUid', 'timestamp', 'prefix', 'imageName')
    tmpID = imageID.select('metadataUid', 'timestamp', 'prefix', 'imageID')\
                    .filter(col('imageID') != '')

    # full outer join on the same prefix, same metadataUid and same timestamp
    imageOutJoin = tmpName.join(tmpID, ['metadataUid', 'timestamp', 'prefix'], 'outer').cache()

    # find out the strictly matched image name and ID
    # [status_containerStatuses_image, status_containerStatuses_imageID]
    # [status_initContainerStatuses_image, status_initContainerStatuses_imageID]
    imageMatch = imageOutJoin.filter(col('imageName').isNotNull() & col('imageID').isNotNull())
    
    # get matchKeyList for later use, ['metadataUid', 'timestamp', 'prefix', 'imageName', 'imageID']
    matchKeyList = imageMatch.schema.names

    # find out the not-matched imageName and imageID
    imageNameNotMatch = imageOutJoin.filter(col('imageID').isNull())
    # we note that all imageIDs are matched 
    imageIDNotMatch = imageOutJoin.filter(col('imageName').isNull()) 

    # we confirm that imageID/imageName + metadataUid can be unique, therefore we omit the group-agg-size workflow
    # if A = {imageName| (imageName, metadataUid) is unique}, B = {imageName| (imageName) is unique}, then B is a subset of A
    # so we consider join on [imageName, metadataUid] before [imageName]
    tmpMatch = imageMatch.select('metadataUid', 'imageName', 'imageID').distinct().cache()
    imageNameLeftJoin = imageNameNotMatch.drop('imageID').join(tmpMatch, ['metadataUid', 'imageName'], 'left').cache()
    imageNameMatch = imageNameLeftJoin.filter(col('imageID').isNotNull())\
                                    .select(matchKeyList)
    imageNameNotMatchAgain = imageNameLeftJoin.filter(col('imageID').isNull()) # about 88% not match 

    # find imageName with a single imageID, and vise versa (they are not equal)
    imageNameUnique = tmpMatch.groupBy('imageName').agg(collect_set('imageID').alias('imageIDs'))\
                            .withColumn('size', size('imageIDs'))\
                            .filter(col('size') == 1)\
                            .select('imageName', explode('imageIDs').alias('imageID'))

    # further match imageNameNotMatchAgain and imageIDNotMatchAgain
    # imageName still not match 86.5%, most from Node 
    imageNameMatchFurther = imageNameNotMatchAgain.drop('imageID')\
                                                .join(imageNameUnique, 'imageName', 'inner')\
                                                .select(matchKeyList)

    imageNameUnion = imageNameMatch.union(imageNameMatchFurther)

    # ['kind', 'key', 'timestamp', 'metahash', 'metadataUid', 'prefix', 'tag', 'tag2', 'imageName', 'imageID']
    resKeyList = [x for x in imageResc.schema.names if x != 'val']
    resKeyList.extend(['imageName', 'imageID'])
    
    # we only take imageMatch for once, specifically, we only join back with imageName
    # namely, we only take [status_containerStatuses_image, status_initContainerStatuses_image]
    imageResult = imageName.join(imageMatch.union(imageNameUnion), ['metadataUid', 'timestamp', 'prefix', 'imageName'], 'left')\
                            .select(resKeyList)
    
    # repeated rows in imageID, we ignore [status_containerStatuses_imageID, status_initContainerStatuses_imageID]
    #imageIDRepeat = imageID.join(imageMatch, ['metadataUid', 'timestamp', 'prefix', 'imageID'], 'left')\
    #                        .select(resKeyList)

    # we find all imageIDs are matched in imageMatch
    # the following code are only for exceptions
    if (imageIDNotMatch.count() > 0):
        imageIDLeftJoin = imageIDNotMatch.drop('imageName').join(tmpMatch, ['metadataUid', 'imageID'], 'left')
        imageIDMatch = imageIDLeftJoin.filter(col('imageName').isNotNull())\
                                    .select(matchKeyList)
        imageIDNotMatchAgain = imageIDLeftJoin.filter(col('imageName').isNull())

        # find imageID with a single imageName
        imageIDUnique = tmpMatch.groupBy('imageID').agg(collect_set('imageName').alias('imageNames'))\
                                .withColumn('size', size('imageNames'))\
                                .filter(col('size') == 1)\
                                .select('imageID', explode('imageNames').alias('imageName'))

        # further match imageIDNotMatchAgain
        imageIDMatchFurther = imageIDNotMatchAgain.drop('imageName')\
                                                .join(imageIDUnique, 'imageID', 'inner')\
                                                .select(matchKeyList)
        imageIDUnion = imageIDMatch.union(imageIDMatchFurther)
        
        #imageUnion = imageUnion.union(imageIDMatch.select(matchKeyList))\
        #                    .union(imageIDMatchFurther.select(matchKeyList))
        
        # we will avoid twice selection in imageMatch, and exclude rows in imageID
        imageIDExclude = imageID.join(imageMatch, ['metadataUid', 'timestamp', 'prefix', 'imageID'], 'left')\
                                .filter(col('imageName').isNull())\
                                .drop('imageName')

        imageIDResult = imageIDExclude.join(imageIDUnion, ['metadataUid', 'timestamp', 'prefix', 'imageID'], 'left')\
                            .select(resKeyList)
        
        imageResult = imageResult.union(imageIDResult) 
    
    return imageResult

#################################################################################################
#                                            Data Ingestion & Processing module for Fraud Analytics                                                                                #
#                                                                                                                                                                                                                                                                                                                                                                                             #
#################################################################################################
############SPARK SUBMITS
#################################################################################################

# pyspark command with jars option: pyspark --jars /mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.spark_spark-streaming-kafka-0-8_2.11-2.1.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.kafka_kafka_2.11-0.8.2.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.spark_spark-tags_2.11-2.1.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.spark-project.spark_unused-1.0.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.scala-lang.modules_scala-xml_2.11-1.0.2.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/com.yammer.metrics_metrics-core-2.2.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.scala-lang.modules_scala-parser-combinators_2.11-1.0.2.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/com.101tec_zkclient-0.3.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.kafka_kafka-clients-0.8.2.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.slf4j_slf4j-api-1.7.16.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/log4j_log4j-1.2.17.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/net.jpountz.lz4_lz4-1.3.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.xerial.snappy_snappy-java-1.1.2.6.jar
# spark-submit command with jars option: 
# spark-submit --master yarn-client --conf spark.dynamicAllocation.enabled=false --jars /mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.spark_spark-streaming-kafka-0-8_2.11-2.1.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.kafka_kafka_2.11-0.8.2.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.spark_spark-tags_2.11-2.1.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.spark-project.spark_unused-1.0.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.scala-lang.modules_scala-xml_2.11-1.0.2.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/com.yammer.metrics_metrics-core-2.2.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.scala-lang.modules_scala-parser-combinators_2.11-1.0.2.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/com.101tec_zkclient-0.3.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.kafka_kafka-clients-0.8.2.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.slf4j_slf4j-api-1.7.16.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/log4j_log4j-1.2.17.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/net.jpountz.lz4_lz4-1.3.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.xerial.snappy_snappy-java-1.1.2.6.jar /mnt/reference/code/testCode/FraudAnalytics_SparkStreaming_v2.3.py 34.201.235.134:2181 fraud_analytics
# spark-submit --master yarn --deploy-mode cluster --name "Data Ingestion & Processing Job for Fraud Analytics" --conf spark.yarn.executor.memoryOverhead=1536 --conf spark.yarn.driver.memoryOverhead=1536 --driver-memory 8g --driver-cores 4 --executor-memory 5g --num-executors 5 --executor-cores 4 --conf spark.dynamicAllocation.enabled=false --jars /usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar,/mnt/reference/code/albertPOC/jars/connectdynamodb_2.11-1.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.spark_spark-streaming-kafka-0-8_2.11-2.1.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.kafka_kafka_2.11-0.8.2.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.spark_spark-tags_2.11-2.1.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.spark-project.spark_unused-1.0.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.scala-lang.modules_scala-xml_2.11-1.0.2.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/com.yammer.metrics_metrics-core-2.2.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.scala-lang.modules_scala-parser-combinators_2.11-1.0.2.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/com.101tec_zkclient-0.3.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.apache.kafka_kafka-clients-0.8.2.1.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.slf4j_slf4j-api-1.7.16.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/log4j_log4j-1.2.17.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/net.jpountz.lz4_lz4-1.3.0.jar,/mnt/reference/code/albertPOC/spark_streaming_kafka_jars/org.xerial.snappy_snappy-java-1.1.2.6.jar /mnt/reference/code/testCode/FraudAnalytics_SparkStreaming_v2.3.py 34.201.235.134:2181 fraud_analytics


import pyspark.sql.types
import sys
import time
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, DataFrame, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import struct
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.classification import LogisticRegressionModel
from kafka import KafkaProducer
from kafka.errors import KafkaError

def getSqlContextInstance(sparkContext):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = \
            SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

# Convert RDDs of the words DStream to DataFrame and run SQL query
def process(time, transactionsDataDF, rdd):
    print '========= %s =========' % time
    print '========= Processing the Kafka Messages Started ========='
    try:

        # Get the singleton instance of SparkSession
        # sqlContext = getSqlContextInstance(rdd.context)

        scNew = rdd.context

        # sqlContext = SQLContext(rdd.context)

        sqlContext = SQLContext(scNew)

        # Convert RDD[String] to RDD[Row] to DataFrame

        rddRow = rdd.map(lambda line: line.split(',')).map(lambda e: \
                Row(creditCardNumber=e[0], paymentAmount=e[1],
                creditCardHolderName=e[2], creditCardCVV=e[3]))
        print '========= createDataFrame of wordsDataFrame ========='
        rddRow.collect()
        df = sqlContext.createDataFrame(rddRow)
        print '========= Printing DataFrame(df) ========='
        df.show()
        recordCount = df.count()
        print 'Printing recordCount: ' + str(recordCount)
        df.registerTempTable('tranCurrentTbl')

        print '========= Printing tranCurrentTbl ========='
        sqlContext.sql('select * from tranCurrentTbl').show()

        # print '========= Printing tranHistoryTbl ========='
        # sqlContext.sql("select * from tranHistoryTbl Card_Number = '7470510000000000'"
        #               ).show(5)

        # joinCount = \
        #    sqlContext.sql('Select a.*, b.* from tranCurrentTbl a,tranHistoryTbl b where a.creditCardNumber = b.Card_Number'
        #                   ).count()

        # finalDF = \
        #    sqlContext.sql('Select a.*, b.* from tranCurrentTbl a,tranHistoryTbl b where a.creditCardNumber = b.Card_Number'
        #                   )

        print '========= Inner join - df, transactionsDataDF Starts Here ========='

        finalDF = transactionsDataDF.join(df, df.creditCardNumber
                == transactionsDataDF.Card_Number)
        finalDF.show(2)

        # finalDF1 = finalDF1.groupBy("Card_Number").agg({{"paymentAmount": "sum"}}).select("Card_Number")

        # transactionsDataDF.registerTempTable('tranHistoryTbl')
        # finalDF = \
        #    sqlContext.sql('select * from tranCurrentTbl c, tranHistoryTbl h where c.creditCardNumber = h.Card_Number'
        #                   )

        print '========= Inner join - df, transactionsDataDF Ends Here ========='

        joinCount = finalDF.count()
        print '========= Printing joinCount ========='
        print joinCount

        finalDF.write.mode('overwrite'
                           ).parquet('/user/hadoop/POC/FraudAnalytics/data/final_result'
                )
        print '========= Printing newDF count ========='
        newDF = \
            sqlContext.read.parquet('/user/hadoop/POC/FraudAnalytics/data/final_result'
                                    )

        # newDF.registerTempTable('newDF')
        # sqlContext.sql('select * from newDF').show()
        # sqlContext.sql('select count(*) from newDF').show()

        newDF.registerTempTable('finalDF')
        sqlContext.sql('select * from finalDF').show()
        query = \
            'SELECT Card_Number, Age, Base_Latitude, Base_Longitude, Credit_limit, Avg_No_of_Transactions, Transaction_Amount, Current_Balance, Merchant_Code, Merchant_Profile_Score, Transaction_Time, Transaction_Latitude, Transaction_Longitude, Distance, No_of_Transactions_Last_24_hrs, Amount_Transacted_Last_24hrs, Transaction_Year, Transaction_Month, Time_Diff_in_hours, Quarterly_Spending FROM finalDF limit 5'
        PredictData = sqlContext.sql(query)
        print '========= Printing PredictData using show ========='
        PredictData.show()

        # sqlContext.registerDataFrameAsTable(PredictData, "PredictData")

        print '========= Preparing dense vectors ========='
        parsedData = PredictData.rdd.map(lambda value: \
                Vectors.dense(value[1:]))
        print '========= Loading LogisticRegressionModel from HDFS ========='
        mymodel = LogisticRegressionModel.load(scNew,
                '/user/hadoop/data/internship/MLIBmodel')
        print '========= Calling predict method ========='
        a = mymodel.predict(parsedData)
        print '========= Taking one value predicted output ========='

        a.collect()
        #print(type(PredictData.rdd.take(0)))
        #print(dir(PredictData.rdd.take(0)))
        df1 = PredictData.select("Card_Number")
        print ' ======== Printing card number ========='
        #Card_Number_Temp = PredictData.rdd.take(0)["Card_Number"]
        #Card_Number_Temp = PredictData.rdd.take(1)
        PredVal = a.take(1)
        print PredVal
        countOfPredict = a.count()
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + str(countOfPredict)
        Outcome = df1.rdd.map(lambda p:p).zip(a)
	rdd1 = Outcome.map(lambda p: Row(allColumns = p[0], predictedValue = int(p[1])))
	rdd1DF = rdd1.toDF()
	sqlContext.registerDataFrameAsTable(rdd1DF, "rdd1DF")
	print(rdd1DF.show())   
	cardNumberQuery = "Select allColumns.*, predictedValue from rdd1DF limit 1"
	cardNumber = sqlContext.sql(cardNumberQuery)
	print(cardNumber.show())
	sqlContext.registerDataFrameAsTable(cardNumber, "cardNumber")
	cardNoPVDFQuery = "select Card_Number, case when Card_Number =8310000000000000 then 1 else 0 END as predictedValue from cardNumber"
	cardNoPVDF = sqlContext.sql(cardNoPVDFQuery)
	print(cardNoPVDF.show())
	cardNoPVDF_1 = cardNoPVDF.collect()
	print(cardNoPVDF_1[0]['Card_Number'])
        #Outcome.collect().foreach(println)

        # card_noLine = newDF.Card_Number

        #card_no = 7672376

        # if (PredVal==0):
        #     producer = KafkaProducer(bootstrap_servers=['34.201.235.134:9092'])
        #    future = producer.send('consumption', b'not fraud')

        print 'Creating KafkaProducer object >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> '
        producer = \
            KafkaProducer(bootstrap_servers=['34.201.235.134:9092'])
        print 'Preparing message string >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> '
        #print str(Card_Number_Temp)
        print str(PredVal[0])
        #message = '{"card_no": "' + str(cardNoPVDF_1[0]['Card_Number']) \
        #    + '", "predicted_value": "' + str(cardNoPVDF_1[0]['predictedValue']) + '"} '
	for row in cardNoPVDF_1:
            print("Printing row >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            print(row)
            message = "card_no:" + str(row['Card_Number']) + "|" + "predicted_value:" + str(row['predictedValue'])
            print message
            print 'Send message string to kafka topic starts >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> '
            #message = 'Testing'
            print message
            future = producer.send('fa_output', message)
            record_metadata = future.get(timeout=10)

        # Successful result returns assigned partition and offset

        #print record_metadata.topic
        #print record_metadata.partition
        #print record_metadata.offset
        print 'Send message string to kafka topic ends >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> '
        print PredVal
        print countOfPredict
        print '========= Processing the Kafka Messages Completed ========='
    except:
        pass

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Usage: FraudAnalytics_SparkStreaming_v1.5.py <zk> <topic>'
        exit(-1)

    spark = \
        SparkSession.builder.appName('Data Ingestion & Processing Job for Fraud Analytics'
            ).getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    # sc = SparkContext(appName='Data Ingestion & Processing Job for Fraud Analytics')

    transactionsDataHDFSPath = \
        '/user/hadoop/POC/FraudAnalytics/data/transactions_history/Transaction_DB_jupyter.csv'
    transactionsDataDF = spark.read.csv(transactionsDataHDFSPath,
            header=True, inferSchema=True)
    print 'Printing transactions history data'
    transactionsDataDF.show(1)
    transactionsDataDF.registerTempTable('tranHistoryTbl')

    # sqlContext.cacheTable("tranHistoryTbl")

    transRecCount = \
        transactionsDataDF.filter("Card_Number = '7470510000000000'"
                                  ).select('*').count()
    print '========= Printing transactionsDataDF count ========='
    print transRecCount

    ssc = StreamingContext(sc, 10)

    # zkQuorum = "34.201.235.134:2181"
    # topic = "fraud_analytics"

    (zkQuorum, topic) = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum,
                                  'fraud_test1_consumer',
                                  {topic: 1})
    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    lines.foreachRDD(lambda rdd: \
                     process(time.strftime('%Y-%m-%d %H:%M:%S'), transactionsDataDF, rdd))
    ssc.start()
    ssc.awaitTermination()

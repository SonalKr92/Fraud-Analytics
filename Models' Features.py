import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pyspark.sql.functions as func
from math import sin, cos, sqrt, atan2, radians, ceil
from pyspark.sql.functions import desc
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import * 
import pyspark.sql
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint

spark = SparkSession.builder.appName('Data Ingestion & Processing Job for Fraud Analytics').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

bankData1 = spark.read.csv("/user/hadoop/POC/FraudAnalytics/data/transactions_history/Transaction_Customer_DB_final.csv", inferSchema = True, header=True)
bankData1.write.mode('overwrite').parquet(' /user/hadoop/POC/FraudAnalytics/data/Model_parquet_file ')
bankData = sqlContext.read.parquet(' /user/hadoop/POC/FraudAnalytics/data/Model_parquet_file ')
bankData.createTempView("dataTable1")


Query = 'select fraud, Card_Number as Card_Num,Transaction_Amount,Credit_limit,Transaction_Timestamp, YEAR(Transaction_Timestamp) as Transaction_Year, MONTH(Transaction_Timestamp) as Transaction_Month, CONCAT(Card_Number,MONTH(Transaction_Timestamp),YEAR(Transaction_Timestamp)) as merge_id from datatable1'
BD = sqlContext.sql(Query)
BD.show(50)
BD.createTempView('Transtbl1')

Q = 'select fraud, YEAR(Transaction_Timestamp) as Transaction_Year, MONTH(Transaction_Timestamp) as Transaction_Month, CONCAT(Card_Number,MONTH(Transaction_Timestamp),YEAR(Transaction_Timestamp)) as merge_id, Customer_Name, Card_Number, Card_Type, Gender, DOB, Age, Base_Location, Base_Latitude, Base_Longitude, Credit_limit, Transaction_Amount, Transaction_Timestamp, Purchase_Type, Merchant_Type, Merchant_Code, Merchant_Profile_Score, Fraud_Risk_of_Merchant_Type from datatable1'
P = sqlContext.sql(Q)
P.createTempView('TempTable')
query = 'select CONCAT(Card_Number,Transaction_Month,Transaction_Year) as merge_id, Transaction_Year, Transaction_Month, Card_Number, sum(Transaction_Amount) as Monthly_Spending, Credit_limit-sum(Transaction_Amount) as Monthly_bal,COUNT(Transaction_Amount) as monthly_trans_count from TempTable group by Transaction_Year, Transaction_Month, Card_Number, Credit_limit'
Qamount = sqlContext.sql(query)
Qamount.show(3)
Qamount.createTempView("bankNew")

query3 = 'select b.merge_id, b.fraud, b.Customer_Name, b.Card_Number, b.Card_Type, b.Gender, b.DOB, b.Age, b.Base_Location, b.Base_Latitude, b.Base_Longitude, b.Credit_limit, b.Transaction_Amount, b.Transaction_Timestamp, b.Purchase_Type, b.Merchant_Type, b.Merchant_Code, b.Merchant_Profile_Score, b.Fraud_Risk_of_Merchant_Type, b.Transaction_Year, b.Transaction_Month, a.Monthly_Spending, a.Monthly_bal, a.monthly_trans_count from bankNew a, TempTable b where a.merge_id = b.merge_id'
bankNew3 = sqlContext.sql(query3)
bankNew3.show(50)
bankNew3.createTempView("bankNew3")


query="SELECT merge_id, Card_Number, Transaction_Amount, Transaction_Timestamp,UNIX_TIMESTAMP(Transaction_Timestamp)- UNIX_TIMESTAMP(LEAD(Transaction_Timestamp, 4,Transaction_Timestamp - interval '2' day) OVER (PARTITION BY Card_Number ORDER BY Transaction_Timestamp DESC)) AS since_last_trans FROM bankNew3 "
FRAME = sqlContext.sql(query)
FRAME.show(10)
FRAME.createTempView("prev_limit_trans")

query="select *, case when since_last_trans < 86400 then 1 else 0 END as daytrans_more_than_four from prev_limit_trans"
FRAME = sqlContext.sql(query)
FRAME.show(10)
FRAME.createTempView("many_transtbl")

query='select a.fraud, a.Credit_limit, a.Merchant_Profile_Score,a.Transaction_Amount,a.monthly_trans_count as Mon_transaction_count, a.Monthly_bal,a.Monthly_Spending, a.Credit_limit/a.Transaction_Amount as Percent_of_CreditLimit, a.Monthly_bal/a.Transaction_Amount as PerTrans_spending FROM many_transtbl b,bankNew3 a where a.Transaction_Timestamp=b.Transaction_Timestamp'
train_data=sqlContext.sql(query)
train_data.show(10)

train_data.createTempView("train_data")
train=train_data.rdd
training = train.map(lambda line: LabeledPoint(line[0],[line[1:]]))
print(training.take(6))
training1 = sqlContext.createDataFrame(training)

model = LogisticRegressionWithLBFGS.train(training)
model.save(sc, "/user/hadoop/POC/FraudAnalytics/test_model2/")
print("model building done")


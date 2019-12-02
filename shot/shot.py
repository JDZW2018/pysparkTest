from pyspark.sql import SparkSession


#PYTHON  找到球员最恐惧的对手

spark = SparkSession.builder.appName("SparkTest").master("local[*]").getOrCreate()
df = spark.read.format("csv").option("header", "true").csv("/Users/mac/workspace/learningSpark/quickStart/src/main/scala/shot/shot_logs.csv")
df.printSchema()
df.createOrReplaceTempView("shot")
rdd = spark.sql("select player_name,CLOSEST_DEFENDER,SHOT_RESULT from shot")\
    .rdd.map(lambda row: fuc(row)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))\
    .filter(lambda x:x[1][0]>5).map(lambda row:fuc2(row))\
    .groupBy(lambda x:x[0])\
    .map(lambda x:min(x[1]))

print(str(rdd.first()))


def fuc(row):
    if(row[2]=="made"):
       return (row[0]+"--"+row[1],(1,1))
    else:
       return (row[0]+"--"+row[1],(1,0))


def fuc2(row):
     pair = str(row[0]).split("--")
     return (pair[0],pair[1],row[1][0],row[1][1]*1.0/row[1][0])


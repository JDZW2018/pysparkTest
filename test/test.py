#导入spark依赖包
from pyspark.sql import SparkSession
#创建sparkSession上下文
spark = SparkSession.builder.appName("test").getOrCreate()
#以csv文件格式，并将文件首行作为元数据读取文件，创建DataFrame
df = spark.read.format("csv").option("header","true").csv("/Users/mac/PycharmProjects/pysparkTest/test/youtube.csv")
#创建内存表test
df.createOrReplaceTempView("test")


#使用标准sql进行计算
df2 =spark.sql("select video_id,(likes-0) as likes ,(dislikes-0) as dislikes,(likes - dislikes) as pureLikes from test order by pureLikes desc ")
df2.printSchema()
#单行排名前100
df2.show(10)
#video_id相同的行pureLikes进行sum
df_sum = df2.groupBy("video_id").sum()
df_sum.show(10)
#根据sum(pureLikes)倒序，显示前100
df_sum.orderBy(df_sum["sum(pureLikes)"].desc()).show(10)
#关闭sparkSession
spark.stop()

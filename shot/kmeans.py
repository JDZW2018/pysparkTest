from pyspark.sql import SparkSession

from pyspark.ml.feature import StringIndexer

spark = SparkSession.builder.appName("SparkTest").master("local[*]").getOrCreate()
df = spark.read.format("csv").option("header", "true").csv("/Users/mac/PycharmProjects/pysparkTest/shot/ticket.csv")
df.printSchema()
df.createOrReplaceTempView("ticket")

df2 = spark.sql("select t.player_name , t.SHOT_DIST,t.CLOSE_DEF_DIST,t.SHOT_CLOCK from shot t ")
#data = df2.filter(df.player_name =='James Harden')

feature = StringIndexer(inputCol="SHOT_DIST", outputCol="SHOT_DIST2")
target = feature.fit(df2).transform(df2)
target.show(10)

from pyspark.sql import Row
from pyspark.ml.clustering import KMeans
from pyspark.mllib.linalg import Vectors


def transData(row):
    return Row(label=row["player_name"],
               features=Vectors.dense([row["SHOT_DIST"],
                                       row["CLOSE_DEF_DIST"],
                                       row["SHOT_CLOCK"],
                                       row["SHOT_DIST2"]]))


# 转化成Dataframe格式
transformed = target.map(transData).toDF()
kmeans = KMeans(k=3)
model = kmeans.fit(transformed)

predict_data = model.transform(transformed)

train_err = predict_data.filter(predict_data['label'] != predict_data['prediction']).count()

total = predict_data.count()


print(float(train_err), total, float(train_err) / total)

#
# #导入数据
# data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),(Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
# df = spark.createDataFrame(data, ["features"])
# #kmeans模型
# kmeans = KMeans(k=2, seed=1)
# model = kmeans.fit(df)
# #簇心数量
# centers = model.clusterCenters()
# len(centers)
# #2
# #训练模型
# transformed = model.transform(df).select("features", "prediction")
# rows = transformed.collect()
# rows[0].prediction == rows[1].prediction
# #True
# rows[2].prediction == rows[3].prediction
# # True
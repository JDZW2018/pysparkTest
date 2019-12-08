from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("work").getOrCreate()
df = spark.read.format("csv").option("header", "true").load("/Users/mac/PycharmProjects/pysparkTest/work1/workload_short.csv")
df1 = spark.read.format("csv").option("header", "true").load("/Users/mac/PycharmProjects/pysparkTest/work1/CPS.csv")
df.createOrReplaceTempView("works")
df1.createOrReplaceTempView("cps")
res0 = spark.sql("select w.race,w.sex,(select count(1) from  cps c where  c.race = w.race) as counts from works w where w.sex ='*' ")

res1 =spark.sql("select  w.race,w.sex,(select count(1) from  cps c where  c.race = w.race and c.sex = w.sex ) as counts from works w where w.sex !='*' ")

res0.show(10)
res1.show(10)




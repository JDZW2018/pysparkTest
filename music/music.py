from pyspark.sql import SparkSession


def fuc(s):
    str =str(s).split("\t")
    print(str)
    return (str[5],1)




if __name__ == '__main__':
    spark = SparkSession.builder.appName("work").master("local[*]").getOrCreate()
    rdd = spark.sparkContext.textFile("/Users/mac/PycharmProjects/pysparkTest/music/nowamagic0000")
    print(str(rdd.first()))
    #第二问
    rdd.map(lambda s:(s,1))
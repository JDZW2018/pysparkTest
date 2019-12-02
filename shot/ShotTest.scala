package shot

import org.apache.spark.sql.SparkSession

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/11/26
  */
object ShotTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SparkTest").master("local[*]").getOrCreate()
    val df =spark.read.format("csv").option("header","true").csv("/Users/mac/workspace/learningSpark/quickStart/src/main/scala/shot/shot_logs.csv")
    df.printSchema()
    df.createOrReplaceTempView("shot")

    //most unwanted defender
    val rdd = spark.sql("select player_name,CLOSEST_DEFENDER,SHOT_RESULT from shot").rdd.map(row=>{
      if(row(2)=="made")
        (row(0)+"--"+row(1),(1,1))
      else
        (row(0)+"--"+row(1),(1,0))
    }).reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)}).filter(_._2._1>5).map(t=>{
      val pair = t._1.split("--")
      (pair(0),pair(1),t._2._1,t._2._2/t._2._1.toDouble)
    }).groupBy(_._1).map(t=>{
      t._2.minBy(_._4)//按最小排序
    })
    rdd.foreach(s=>println(s))


    //comfortable zone ： James Harden
  }
}

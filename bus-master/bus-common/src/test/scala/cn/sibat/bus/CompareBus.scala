package cn.sibat.bus

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/6/19.
  */
object CompareBus {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("G").master("local[*]").getOrCreate()
    import spark.implicits._
    val old = spark.read.textFile("D:\\testData\\公交处\\busArrival\\2016-12-01").map{s=>
      s.split(",")(0)+","+s.split(",")(4)
    }.distinct().map(s=>(s.split(",")(0),1)).rdd.reduceByKey(_+_)
    val now = spark.read.textFile("D:\\testData\\公交处\\toStation9").map(s=>{
      val split = s.split(",")
      "粤"+split(3).substring(2)+","+split(split.length-1)
    }).distinct().rdd.map(s=>(s.split(",")(0),1)).reduceByKey(_+_)

//    val join = old.join(now)
//    val count = join.count()
//    val oldMax = join.filter(t=> t._2._1 > t._2._2).count()
//    val nowMax = join.filter(t=> t._2._1 < t._2._2).count()
//    val equals = join.filter(t=> t._2._1 == t._2._2).count()
//
//    println(oldMax,oldMax.toDouble/count,nowMax,nowMax.toDouble/count,equals,equals.toDouble/count)

    val fullJoin = old.fullOuterJoin(now)
    val fullCount = fullJoin.count()
    val fullOld = fullJoin.filter(s=>s._2._1.isEmpty).count()
    val fullNow = fullJoin.filter(s=>s._2._2.isEmpty).count()

    println(fullOld,fullOld.toDouble/fullCount,fullNow,fullNow.toDouble/fullCount)
  }
}

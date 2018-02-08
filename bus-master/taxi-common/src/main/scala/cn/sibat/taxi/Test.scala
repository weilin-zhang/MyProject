package cn.sibat.taxi

import java.text.SimpleDateFormat

import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by kong on 2017/4/17.
  */
object Test {

  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().master("local[*]").appName("t").getOrCreate()
//    spark.read.option("header",true).option("inferSchema",true).csv("D:\\testData\\公交处\\data\\taxi_op\\taxi_op*.csv")
//        .toDF("termId","carId","upTime","downTime","singlePrice","runningDistance","runTime","sumPrice","emptyDistance","name","carType","aDate").show()
//    spark.read.parquet("D:/testData/公交处/taxiDeal/2017_03_00").show()
//    spark.read.parquet("D:/testData/公交处/taxiDeal/2016_12_00").filter(col("date") === "2016-10-01").show()
    val a = Array(1,2)
    for (i <- a.indices if i % 3 == 0){
      println(a.slice(i,i+3).mkString(","))
    }
  }
}

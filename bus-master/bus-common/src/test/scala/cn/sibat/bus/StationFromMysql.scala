package cn.sibat.bus

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 从mysql中读取站点静态数据
  * Created by kong on 2017/5/3.
  */
object StationFromMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("DirectNULLToPredict").master("local[*]").getOrCreate()
    val url = "jdbc:mysql://xxx"
    val prop = new Properties()
    val lineDF = spark.read.jdbc(url, "line", prop)
    val lineStopDF = spark.read.jdbc(url, "line_checkpoint", prop)
    lineDF.createOrReplaceTempView("line")
    lineStopDF.createOrReplaceTempView("line_checkpoint")
    val sql = "select l.ref_id,l.direction,c.lon,c.lat,c.point_order from line l,line_checkpoint c where l.id=c.line_id order by l.ref_id,l.direction,c.point_order"
    val checkPoint = spark.sql(sql)
    checkPoint.write.parquet("D:/testData/公交处/checkpointV2")
  }
}

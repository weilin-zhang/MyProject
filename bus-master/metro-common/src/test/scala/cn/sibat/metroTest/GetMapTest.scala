package cn.sibat.metroTest

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by wing1995 on 2017/8/29.
  */
object GetMapTest extends App {

    val spark = SparkSession.builder().appName("CarFreeDayAPP").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my").getOrCreate()
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\subway_station")

    val stationMap = new mutable.HashMap[String, String]()
    ds.foreach(line => {
        val arr = line.split(",")
        stationMap.+=((arr(0), arr(1)))
    })
    System.out.println(stationMap.size)
    for(map <- stationMap) {
        System.out.println(map._1, map._2)
    }
}

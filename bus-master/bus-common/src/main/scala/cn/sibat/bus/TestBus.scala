package cn.sibat.bus

import java.io.File
import javax.xml.parsers.{DocumentBuilder, DocumentBuilderFactory}

import org.apache.spark.sql.{Row, SparkSession}
import org.w3c.dom.Node

import scala.collection.mutable.ArrayBuffer

case class TestBus(id: String, num: String, or: String)

/**
  * hh
  * Created by kong on 2017/4/10.
  */
object TestBus {

  def Frechet(): Unit = {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "/user/kongshaohong/spark-warehouse/").appName("BusCleanTest").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val start = System.currentTimeMillis()
    //    val data = spark.read.textFile("D:/testData/公交处/data/STRING_20170704").filter(_.split(",").length > 14)
    //    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
    //    val format = busDataCleanUtils.dataFormat().filterStatus().errorPoint().upTimeFormat("yy-M-d H:m:s").filterErrorDate().toDF
    val format = spark.read.parquet("/user/kongshaohong/bus/data/20170704Format/")

    val groupByKey = format.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    val station = spark.read.textFile("/user/kongshaohong/bus/lineInfo.csv").map { str =>
      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
      import cn.sibat.bus.utils.LocationUtil
      val Array(lat, lon) = LocationUtil.gcj02_To_84(stationLat.toDouble, stationLon.toDouble).split(",")
      new StationData(route, direct, stationId, stationName, stationSeqId.toInt, lon.toDouble, lat.toDouble)
    }.collect()
    //val bStation = spark.sparkContext.broadcast(station)

    val mapStation = station.groupBy(sd => sd.route + "," + sd.direct)
    val bMapStation = spark.sparkContext.broadcast(mapStation)

    groupByKey.flatMapGroups((s, it) => {
      val gps = it.toArray[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))
      val stationMap = bMapStation.value
      val result = new ArrayBuffer[String]()
      val lon_lat = new ArrayBuffer[Point]()
      gps.foreach { row =>
        val split = row.split(",")
        val lon = split(8).toDouble
        val lat = split(9).toDouble
        val time = split(11)
        val lineId = split(4)
        lon_lat.+=(Point(lon, lat))
        println(lon_lat.length)
        val stationData = stationMap.getOrElse(lineId + ",up", Array()).map(sd => Point(sd.stationLon, sd.stationLat))
        val frechetDis = FrechetUtils.compareGesture(lon_lat.toArray, stationData)
        result += row + "," + frechetDis
      }
      result
    }).rdd.saveAsTextFile("/user/kongshaohong/bus/frechetAll")

    val end = System.currentTimeMillis()

    println("开始程序时间:" + start + ";结束时间:" + end + ";耗时:" + (end - start) / 1000 + "s!!!")
  }


  def main(args: Array[String]): Unit = {
    val data = "113.908439,22.482539;\n113.908021,22.482143;\n113.908477,22.483114;\n113.908956,22.482767;\n113.908365,22.483066;\n113.908757,22.48358;\n113.908394,22.482901;\n113.909539,22.48304;\n113.908526,22.48304;\n113.908285,22.482853;\n113.906366,22.484611;\n113.908259,22.482254;\n113.908624,22.483146;".split("\n")

    var sum = (0.0, 0.0)
    data.foreach(s=>{
      val Array(lon,lat) = s.replace(";","").split(",").map(_.toDouble)
      sum = sum.copy(sum._1 + lon,sum._2 + lat)
    })

    println(sum._1/data.length,sum._2/data.length)
  }
}

package cn.sibat.bus

import cn.sibat.bus.utils.LocationUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 测试BusDataCleanUtil的方法
  * 都通过
  * Created by kong on 2017/5/2.
  */
object BusCleanTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("BusCleanTest").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val start = System.currentTimeMillis()
//    val data = spark.read.textFile("D:/testData/公交处/data/STRING_20170704").filter(_.split(",").length > 14)
//    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
//    val format = busDataCleanUtils.dataFormat().filterStatus().errorPoint().upTimeFormat("yy-M-d H:m:s").filterErrorDate().toDF
    val format = spark.read.parquet("D:/testData/公交处/20170704Format/")

    val groupByKey = format.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    val station = spark.read.textFile("D:/testData/公交处/lineInfo.csv").map { str =>
      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
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
        val stationData = stationMap.getOrElse(lineId + ",up", Array()).map(sd => Point(sd.stationLon, sd.stationLat))
        val frechetDis = FrechetUtils.compareGesture1(lon_lat.toArray, stationData)
        result += row+","+frechetDis
      }
      result
    })
      .rdd.saveAsTextFile("D:/testData/公交处/frechetAll")

    val end = System.currentTimeMillis()

    println("开始程序时间:"+start+";结束时间:"+end+";耗时:"+(end-start)/1000+"s!!!")

    //    println(format.data.count())
    //    println(format.zeroPoint().data.count())
    //    format.zeroPoint().data.show()
    //println(format.errorPoint().data.count())
    //format.errorPoint().data.show()
    //    println(format.filterStatus().data.count())
    //    format.filterStatus().data.show()
    //format.intervalAndMovement().data.show()
  }
}

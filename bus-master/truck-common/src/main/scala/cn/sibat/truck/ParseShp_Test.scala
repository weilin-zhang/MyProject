package cn.sibat.truck

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import cn.sibat.bus.utils.LocationUtil

/**
  * 将地铁、公交、出租 OD 数据中的经纬度 转换成区域
  * Created by will on 2017/12/06.
  */
object ParseShp_Test {

  //Logger.getLogger("org").setLevel(Level.ERROR)
  //System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0\\hadoop-common-2.2.0-bin")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("subway/bus/change Data Processing")
//                .setMaster("local[*]")
                .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")

    val spark = SparkSession.builder().config(conf).getOrCreate()

//    var dataPath1 = "Mining/SubwayOD_in_one_line/"+args(0)

//    var dataPath1 = "Mining/BusD_new/BusD/"+args(0) + "/*/*"
//    var dataPath1 = "Mining/BusD_withouttimeField/"+args(0)   //  读取bus 的数据要注意有一部分数据是没有d_time 的，要分开处理
    var dataPath1 = "Mining/Bus_Subway_Change/subway2bus/"+args(0)
//    val dataPath1 = "E:\\BusOD\\2016-12-01"
    val df1 = spark.read.textFile(dataPath1)

    import df1.sparkSession.implicits._
    /**
      * 注意 shp 文件要放在根目录下
      */
    val shpPath1 = "xzq.shp"//不同等级下划分的区域shp文件路径
    val shpPath2 = "jd.shp"
    val shpPath3 = "jtxq.shp"

//    val shpPath1 = "E:/trafficDataAnalysis/guotu/行政区划2017/英文/xzq.shp"//不同等级下划分的区域shp文件路径
//    val shpPath2 = "E:/trafficDataAnalysis/guotu/行政区划2017/英文/jd.shp"
//    val shpPath3 = "E:/trafficDataAnalysis/guotu/行政区划2017/英文/jtxq.shp"

    val parseShp1 = ParseShp.apply(shpPath1).readShp()
    val parseShp2 = ParseShp.apply(shpPath2).readShp()
    val parseShp3 = ParseShp.apply(shpPath3).readShp()

    val getZoneName = udf {(lon: Double,lat:Double) =>{
      val Array(lat1, lon1) = LocationUtil.gcj02_To_84(lat,lon).split(",")
      val zone = parseShp1.getZoneName(lon1.toDouble,lat1.toDouble)
      zone
    }}

    val getStreetName = udf {(lon: Double,lat:Double) =>{
      val Array(lat1, lon1) = LocationUtil.gcj02_To_84(lat,lon).split(",")
      val zone = parseShp2.getZoneName(lon1.toDouble,lat1.toDouble)
      zone
    }}

    val getTrafficName = udf {(lon: Double,lat:Double) =>{
      val Array(lat1, lon1) = LocationUtil.gcj02_To_84(lat,lon).split(",")
      val zone = parseShp3.getZoneName(lon1.toDouble,lat1.toDouble)
      zone
    }}

//    var savePath1 = "data2/subway/"+args(0)
//    df1.map(line => {
//      val arr = line.split(",")
//      Subway(arr(0), arr(1), arr(2), arr(3), arr(4).toDouble, arr(5).toDouble, arr(6), arr(7), arr(8),
//        arr(9).toDouble, arr(10).toDouble)
//    }).toDF()

//    var savePath1 = "data2/bus/"+args(0)
//    df1.map(line => {
//      val arr = line.split(",")
//      bus(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8),
//        arr(9).toDouble, arr(10).toDouble, arr(11), arr(12), arr(13), arr(14), arr(15).toDouble, arr(16).toDouble)
//    }).toDF()
//      .withColumn("o_ZoneName", getZoneName(col("o_lon"), col("o_lat")))
//      .withColumn("o_StreetName", getStreetName(col("o_lon"), col("o_lat")))
//      .withColumn("o_TrafficName", getTrafficName(col("o_lon"), col("o_lat")))
//      .withColumn("d_ZoneName", getZoneName(col("d_lon"), col("d_lat")))
//      .withColumn("d_StreetName", getStreetName(col("d_lon"), col("d_lat")))
//      .withColumn("d_TrafficName", getTrafficName(col("d_lon"), col("d_lat")))
//      .map(_.mkString(",")).rdd.repartition(1).saveAsTextFile(savePath1)



    var savePath1 = "data2/change/subway2bus/"+args(0)
    df1.map(arr => {
      val line = arr.split(",")
      Change(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7).toInt, line(8).toDouble,
        line(9).toDouble, line(10), line(11), line(12),line(13), line(14).toInt, line(15).toDouble, line(16).toDouble,line(17), line(18), line(19), line(20),
        line(21), line(22), line(23).toInt, line(24).toDouble,line(25).toDouble, line(26), line(27), line(28),line(29), line(30), line(31).toDouble, line(32).toDouble,
        line(33).toLong, line(34).toDouble)
    }).toDF()
      .withColumn("o_ZoneName", getZoneName(col("o_lon"), col("o_lat")))
      .withColumn("o_StreetName", getStreetName(col("o_lon"), col("o_lat")))
      .withColumn("o_TrafficName", getTrafficName(col("o_lon"), col("o_lat")))
      .withColumn("d_ZoneName", getZoneName(col("d_lon"), col("d_lat")))
      .withColumn("d_StreetName", getStreetName(col("d_lon"), col("d_lat")))
      .withColumn("d_TrafficName", getTrafficName(col("d_lon"), col("d_lat")))
      .withColumn("o_ZoneName2", getZoneName(col("o_lon2"), col("o_lat2")))
      .withColumn("o_StreetName2", getStreetName(col("o_lon2"), col("o_lat2")))
      .withColumn("o_TrafficName2", getTrafficName(col("o_lon2"), col("o_lat2")))
      .withColumn("d_ZoneName2", getZoneName(col("d_lon2"), col("d_lat2")))
      .withColumn("d_StreetName2", getStreetName(col("d_lon2"), col("d_lat2")))
      .withColumn("d_TrafficName2", getTrafficName(col("d_lon2"), col("d_lat2")))
      .map(_.mkString(",")).rdd.repartition(1).saveAsTextFile(savePath1)

    spark.stop()
  }
}

case class taxi_OD(carId: String, recordTimeO: String, recordTimeD: String, o_lon: Double, o_lat: Double,
                   d_lon: Double, d_lat: Double,diffs_time: Long, dist: Double)
case class taxi(carId:String,recordTime:String,lon: Double,lat: Double,type1:String)

case class bus(card_id: String,line:String,car_id:String,direction:String,devide:String,o_time: String,o_station_id:String,
               o_station_name: String, o_station_index: String,o_lon: Double, o_lat: Double, d_time: String,
               d_station_id: String, d_station_name: String,d_station_index:String, d_lon: Double, d_lat: Double
              )


case class Bus_Static(line_id: String,direction:String,station_id:String,station_name:String,stop_order:Long,lat: Double, lon: Double)

case class Bus_Static_new(station_id:String,station_name:String,lat: Double, lon: Double)

case class Subway(card_id: String, o_time: String, o_line: String, o_station_name: String, o_lon: Double, o_lat: Double,
                  d_time: String, d_line: String, d_station_name: String, d_lon: Double, d_lat: Double)

case class Subway_Static(station_id: String,station_name:String,line_id:String,line_name:String)

case class Change(card_id:String,type1:String,line11:String,devide1:String,o_time:String,o_station_id:String,o_station_name:String,o_station_index:Int,
                  o_lon: Double,o_lat: Double,line12:String,d_time:String,d_station_id:String,d_station_name:String,d_station_index:Int,
                  d_lon: Double,d_lat: Double,type2:String,line21:String,devide2:String,o_time2:String, o_station_id2:String,o_station_name2:String,o_station_index2:Int,
                  o_lon2: Double,o_lat2: Double,line22:String,d_time2:String,d_station_id2:String,d_station_name2:String,d_station_index2:String,
                  d_lon2: Double,d_lat2: Double,time_diff:Long,dist:Double)
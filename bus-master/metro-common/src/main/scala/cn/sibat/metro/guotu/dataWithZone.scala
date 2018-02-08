package cn.sibat.metro.guotu


import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import cn.sibat.bus.utils.LocationUtil
import cn.sibat.truck.ParseShp




class dataWithZone extends Serializable {

//  val shpPath1 = "xzq.shp"//不同等级下划分的区域shp文件路径
//  val shpPath2 = "jd.shp"
//  val shpPath3 = "jtxq.shp"

//  val shpPath1 = "E:/trafficDataAnalysis/guotu/行政区划2017/英文/xzq.shp"//不同等级下划分的区域shp文件路径
//  val shpPath2 = "E:/trafficDataAnalysis/guotu/行政区划2017/英文/jd.shp"
//  val shpPath3 = "E:/trafficDataAnalysis/guotu/行政区划2017/英文/jtxq.shp"
  val shpPath1 = "xzq.shp"//不同等级下划分的区域shp文件路径
  val shpPath2 = "jd.shp"
  val shpPath3 = "jtxq.shp"

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

  /**
    * 数据格式化，将深圳通数据计算出来的公交OD数据格式化
    * @param ds
    * @return df
    */
  def formatUtilBus(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 17)
      .map(arr => bus(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9).toDouble,
        arr(10).toDouble, arr(11), arr(12), arr(13), arr(14), arr(15).toDouble, arr(16).toDouble)).toDF()
    filteredDf
  }

  /**
    * 数据格式化，将深圳通数据计算出来的地铁OD数据格式化
    * @param ds
    * @return df
    */
  def formatUtilSubway(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 11)
      .map(arr =>Subway(arr(0), arr(1), arr(2), arr(3), arr(4).toDouble, arr(5).toDouble, arr(6), arr(7), arr(8),
        arr(9).toDouble, arr(10).toDouble)).toDF()
    filteredDf
  }

  /**
    * 数据格式化，将深圳通数据计算出来的换乘数据格式化
    * @param ds
    * @return df
    */
  def formatUtilChange(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 35)
      .map(line =>Change(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7).toInt, line(8).toDouble,
        line(9).toDouble, line(10), line(11), line(12),line(13), line(14).toInt, line(15).toDouble, line(16).toDouble,
        line(17), line(18), line(19), line(20), line(21), line(22), line(23).toInt, line(24).toDouble,line(25).toDouble,
        line(26), line(27), line(28),line(29), line(30), line(31).toDouble, line(32).toDouble, line(33).toLong, line(34).toDouble)).toDF()
    filteredDf
  }

  /**
    * 数据格式化，将深圳通数据计算出来的出租数据格式化
    * @param ds
    * @return df
    */
  def formatUtilTaxi(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 5)
      .map(arr => taxi(arr(0), arr(1), arr(2).toDouble, arr(3).toDouble, arr(4))).toDF()
    filteredDf
  }

  /**
    * 数据格式化，将深圳通数据计算出来的出租OD数据格式化
    * @param ds
    * @return df
    */
  def formatUtilTaxiOD(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 9)
      .map(arr => taxi_OD(arr(0), arr(1), arr(2), arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble,
        arr(7).toLong, arr(8).toDouble)).toDF()
    filteredDf
  }
  /**
    * 数据格式化，将深圳通数据计算出来的出租2014年12月OD数据格式化
    * @param ds
    * @return df
    */
  def formatUtilTaxi_ODnew(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 12)
      .map(arr => taxi_ODnew(arr(0), arr(1), arr(2), arr(3).toLong, arr(4).toDouble, arr(5).toDouble, arr(6),
        arr(7), arr(8).toLong,arr(9).toDouble,arr(10).toDouble,arr(11).toDouble)).toDF()
    filteredDf
  }

  /**
    * 数据格式化，将地铁Static数据格式化
    * @param ds
    * @return df
    */
  def formatUtilSubway_Static(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 4)
      .map(arr => Subway_Static(arr(0), arr(1), arr(2), arr(3))).toDF()
    filteredDf
  }

  /**
    * 添加区域（行政区、街道、交通小区）字段,针对原始数据
    * @param df
    * @return df
    */
  def addZone(df:DataFrame):DataFrame = {
    df.withColumn("ZoneName", getZoneName(col("lon"), col("lat")))
      .withColumn("StreetName", getStreetName(col("lon"), col("lat")))
      .withColumn("TrafficName", getTrafficName(col("lon"), col("lat")))
  }

  /**
    * 添加区域（行政区、街道、交通小区）字段,针对bus和subway OD数据
    * @param df
    * @return df
    */
  def addODZone(df:DataFrame):DataFrame = {
    df.withColumn("o_ZoneName", getZoneName(col("o_lon"), col("o_lat")))
      .withColumn("o_StreetName", getStreetName(col("o_lon"), col("o_lat")))
      .withColumn("o_TrafficName", getTrafficName(col("o_lon"), col("o_lat")))
      .withColumn("d_ZoneName", getZoneName(col("d_lon"), col("d_lat")))
      .withColumn("d_StreetName", getStreetName(col("d_lon"), col("d_lat")))
      .withColumn("d_TrafficName", getTrafficName(col("d_lon"), col("d_lat")))
  }

  /**
    * 添加区域（行政区、街道、交通小区）字段,针对换乘数据
    * @param df
    * @return df
    */
  def addChangeZone(df:DataFrame):DataFrame = {
    df.withColumn("o_ZoneName", getZoneName(col("o_lon"), col("o_lat")))
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
  }
}

object dataWithZone{

  System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0\\hadoop-common-2.2.0-bin")
  def apply: dataWithZone = new dataWithZone()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("subway/bus/change Data Processing")
//      .setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._    //添加隐式转换
//    val dataPath1 = "E:\\BusOD\\2016-12-01\\*\\*"
//    val df1 = spark.read.textFile(dataPath1)
//    val df2 = dataWithZone.apply.formatUtilBus(df1).toDF()
//    val df3 = dataWithZone.apply.addODZone(df2)
//    var savePath1 = "E:\\BusOD\\2016-12-01_zone"
//    df3.map(_.mkString(",")).rdd.repartition(1).saveAsTextFile(savePath1)

    val dataPath1 = "/user/zhangjun/taxi_od/"+args(0)
    val df1 = spark.read.textFile(dataPath1)
    val df2 = dataWithZone.apply.formatUtilTaxi_ODnew(df1).toDF()
    val df3 = dataWithZone.apply.addODZone(df2)
    var savePath1 = "data2/taxi_od/"+args(0)
    df3.map(_.mkString(",")).rdd.repartition(1).saveAsTextFile(savePath1)

    spark.stop()


  }
}


case class bus(card_id: String,line:String,car_id:String,direction:String,devide:String,o_time: String,o_station_id:String,
               o_station_name: String, o_station_index: String,o_lon: Double, o_lat: Double, d_time: String,
               d_station_id: String, d_station_name: String,d_station_index:String, d_lon: Double, d_lat: Double)

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

case class taxi(carId:String,recordTime:String,lon: Double,lat: Double,type1:String)
case class taxi_OD(carId: String, recordTimeO: String, recordTimeD: String, o_lon: Double, o_lat: Double,
                   d_lon: Double, d_lat: Double,diffs_time: Long, dist: Double)

case class taxi_ODnew(carid: String, uptime: String, GPSuptime: String,uptimediff:Long, o_lon: Double, o_lat: Double,downtime:String,GPSdowntime:String,downtimediff:Long,d_lon: Double, d_lat: Double, dist: Double)



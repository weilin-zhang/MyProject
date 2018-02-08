package cn.sibat.metro.guotu


import cn.sibat.metro.utils.TimeUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.log4j.{Level, Logger}
import scala.io.Source



class subwayFlow extends Serializable {

  /**
    * 数据格式化，将清分数据按照线路条数不同分别进行格式化
    * @param ds
    * @return df
    */
  def formatUtilQingFen(ds: Dataset[String]): DataFrame = {
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 4)
      .map(arr => qingFenOD(arr(0), arr(1), arr(2), arr(3).toDouble )).toDF()
    filteredDf
  }

  def formatUtilQingFen2(ds: Dataset[String]): DataFrame = {
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 5)
      .map(arr => qingFenOD2(arr(0), arr(1), arr(2), arr(3), arr(4).toDouble)).toDF()
    filteredDf
  }

  def formatUtilQingFen3(ds: Dataset[String]): DataFrame = {
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 6)
      .map(arr => qingFenOD3(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5).toDouble)).toDF()
    filteredDf
  }

  def formatUtilQingFen4(ds: Dataset[String]): DataFrame = {
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 7)
      .map(arr => qingFenOD4(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6).toDouble)).toDF()
    filteredDf
  }

  def formatUtilSubway_withZone(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 17)
      .map(arr =>Subway_withZone(arr(0), arr(1), arr(2), arr(3), arr(4).toDouble, arr(5).toDouble, arr(6), arr(7), arr(8),
        arr(9).toDouble, arr(10).toDouble,arr(11),arr(12),arr(13),arr(14),arr(15),arr(16))).toDF()
    filteredDf
  }

  /**
    * 根据站点id 添加站点名称
    * 采用 Source.fromFile 的方法读取静态数据，缺点是只能读本地的文件，不能读集群的
    * @param id 站点id
    * @param path 地铁静态数据path
    * @return stationName
    */
  def stationId2Name(id:String,path:String):String={
    val data = Source.fromFile(path)
    val id_name = scala.collection.mutable.Map[String,String]()
    data.getLines().foreach(elem=>{
      val s = elem.split(",")
      val id = s(0)
      val name = s(1)
      id_name.put(id,name)
    })
    id_name(id)
  }

  /**
    * 根据线路id 添加站点名称
    * 采用 Source.fromFile 的方法读取静态数据，缺点是只能读本地的文件，不能读集群的
    * @param id 线路id
    * @param path 地铁静态数据path
    * @return lineName
    */
  def lineId2Name(id:String,path:String):String={
    val data = Source.fromFile(path)
    val id_name = scala.collection.mutable.Map[String,String]()
    data.getLines().foreach(elem=>{
      val s = elem.split(",")
      val id = s(2)
      val name = s(3)
      id_name.put(id,name)
    })
    id_name(id)
  }

  /**
    * 为不同清分数据添加stationName、lineName 两个字段
    * @param df
    * @param path_static 地铁静态数据path
    * @return df
    */
  def addNameField(df:DataFrame,path_static:String):DataFrame={
    import df.sparkSession.implicits._
    df.map(line=>{
      val o_name = stationId2Name(line.getString(0),path_static)
      val d_name = stationId2Name(line.getString(1),path_static)
      val line_name =lineId2Name(line.getString(2),path_static)
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line_name,line.getDouble(3))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line_id","line_name","prob")
  }

  def addNameField2(df:DataFrame,path_static:String):DataFrame={
    import df.sparkSession.implicits._
    df.map(line=>{
      val o_name = stationId2Name(line.getString(0),path_static)
      val d_name = stationId2Name(line.getString(1),path_static)
      val line1_name =lineId2Name(line.getString(2),path_static)
      val line2_name = lineId2Name(line.getString(3),path_static)
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line1_name,line.getString(3),line2_name,line.getDouble(4))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line1_id","line1_name","line2_id","line2_name","prob")
  }

  def addNameField3(df:DataFrame,path_static:String):DataFrame={
    import df.sparkSession.implicits._
    df.map(line=>{
      val o_name = stationId2Name(line.getString(0),path_static)
      val d_name = stationId2Name(line.getString(1),path_static)
      val line1_name =lineId2Name(line.getString(2),path_static)
      val line2_name = lineId2Name(line.getString(3),path_static)
      val line3_name = lineId2Name(line.getString(4),path_static)
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line1_name,line.getString(3),line2_name,
        line.getString(4),line3_name, line.getDouble(5))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line1_id","line1_name","line2_id","line2_name",
      "line3_id","line3_name","prob")
  }

  def addNameField4(df:DataFrame,path_static:String):DataFrame={
    import df.sparkSession.implicits._
    df.map(line=>{
      val o_name = stationId2Name(line.getString(0),path_static)
      val d_name = stationId2Name(line.getString(1),path_static)
      val line1_name =lineId2Name(line.getString(2),path_static)
      val line2_name = lineId2Name(line.getString(3),path_static)
      val line3_name = lineId2Name(line.getString(4),path_static)
      val line4_name = lineId2Name(line.getString(5),path_static)
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line1_name,line.getString(3),line2_name,
        line.getString(4),line3_name,line.getString(5),line4_name, line.getDouble(6))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line1_id","line1_name","line2_id","line2_name",
      "line3_id","line3_name","line4_id","line4_name","prob")
  }

  /**
    * 添加timeMid 字段，取o_time和d_time的中间时间 timeMid 作为高峰的过滤时间
    * @param df
    * @return
    */
  def addTimeMid(df:DataFrame):DataFrame={
    import df.sparkSession.implicits._
    df.map(line=> {
      val Otime = line.getString(line.fieldIndex("o_time")).substring(11, 19)
      val Dtime = line.getString(line.fieldIndex("d_time")).substring(11, 19)
      val OtimeStamp = TimeUtils.apply.date2Stamp(TimeUtils.apply.time2Date(Otime, "HH:mm:ss"))
      val DtimeStamp = TimeUtils.apply.date2Stamp(TimeUtils.apply.time2Date(Dtime, "HH:mm:ss"))
      val timeMid = TimeUtils.apply.stamp2Date(OtimeStamp+(DtimeStamp-OtimeStamp)/2).toString.substring(11,19)
      (line.getString(line.fieldIndex("card_id")), timeMid, line.getString(line.fieldIndex("o_line")), line.getString(line.fieldIndex("o_station_name")),
        line.getString(line.fieldIndex("d_line")),  line.getString(line.fieldIndex("d_station_name")))
    }).toDF("card_id","timeMid","o_line","o_station_name","d_line","d_station_name")
  }

  /**
    * 将SZT 数据过滤出早高峰数据
    * @param df
    * @return
    */
  def filterZao(df:DataFrame):DataFrame={
    df.filter(row => {
      val time_zao = row.getString(row.fieldIndex("timeMid"))
      time_zao.matches("08:.*")
    })
  }
  /**
    * 将SZT 数据过滤出晚高峰数据
    * @param df
    * @return
    */
  def filterWan(df:DataFrame):DataFrame={
    df.filter(row => {
      val time_wan = row.getString(row.fieldIndex("timeMid"))
      time_wan.matches("18:.*")
    })
  }
  /**
    * 将SZT 数据过滤出平峰数据
    * @param df
    * @return
    */
  def filterOther(df:DataFrame):DataFrame={
    df.filter(row => {
      val time = row.getString(row.fieldIndex("timeMid"))
      !(time.matches("08:.*") || time.matches("18:.*"))
    })
  }

  /**
    * 根据起止站点名称统计OD 数量
    * @param df
    * @return
    */
  def countOD(df:DataFrame):DataFrame={
    df.groupBy(col("o_station_name"),col("d_station_name")).count()
  }

  def countOD_zone(df:DataFrame):DataFrame={
    df.groupBy(col("o_station_name"),col("d_station_name"),col("o_zone")).count()
  }

  /**
    * 获得不同时段线路客流量。利用清分数据分别和不同时段OD数据进行join,（客流量 = OD数 * 概率） 再求和
    * @param df_qingFen 添加了name 字段的清分数据
    * @param df_OD  OD的统计数据
    * @return
    */
  def countLineFlow(df_qingFen:DataFrame,df_OD:DataFrame):DataFrame={
    df_qingFen.join(df_OD,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line_name").sum("num")
  }

  def countLineFlow2(df_qingFen:DataFrame,df_OD:DataFrame):DataFrame={
    df_qingFen.join(df_OD,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line1_name","line2_name").sum("num")
  }
  def countLineFlow3(df_qingFen:DataFrame,df_OD:DataFrame):DataFrame={
    df_qingFen.join(df_OD,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line1_name","line2_name","line3_name").sum("num")
  }
  def countLineFlow4(df_qingFen:DataFrame,df_OD:DataFrame):DataFrame={
    df_qingFen.join(df_OD,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line1_name","line2_name","line3_name","line4_name").sum("num")
  }
}

object subwayFlow {
  def apply: subwayFlow = new subwayFlow()

  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0\\hadoop-common-2.2.0-bin")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]") //本地模式
      .appName("subwayFlowCount")
      .getOrCreate()


    //代码功能：根据清分来计算地铁线路客流量
    val path = "E:\\trafficDataAnalysis\\subway\\2016-12-01"
    val data_source = spark.read.textFile(path)
    val path_static = "E:\\trafficDataAnalysis\\subway_station.station"
    val data_static = spark.read.textFile(path_static)
    val path_qingFen = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan"
    val data_qingFen = spark.read.textFile(path_qingFen)

    val data_ds= dataWithZone.apply.formatUtilSubway(data_source)
    val static_ds = dataWithZone.apply.formatUtilSubway_Static(data_static)

    /**
      * 为清分数据添加name 字段，以仅换乘一次为例，
      * 换乘 n 次的只需更改formatUtilQingFen2() ,addNameField2() 方法
      */
    val qingFen_ds = subwayFlow.apply.formatUtilQingFen2(data_qingFen).toDF()
    val qingFen_name = subwayFlow.apply.addNameField2(qingFen_ds,path_static)

//    qingFen_name.map(_.mkString(",")).rdd.repartition(1)
//      .saveAsTextFile("C:\\Users\\will\\Desktop\\worday_name\\wan\\workday_2")


    val data_format = subwayFlow.apply.addTimeMid(data_ds)
    /**
      * 以晚高峰为例,得到换乘一次的线路客流量
      * 其他时间段只需更改 filterWan()方法
      * 要得到换乘 n 次的要更改 countLineFlow() 方法中的 groupBy("line1_name","line2_name",.....)
      *     例如不换乘的 groupBy("line_name")
      */
    val data_wan = subwayFlow.apply.filterWan(data_format)
    val data_grp = subwayFlow.apply.countOD(data_wan)
    val qingFen_result = subwayFlow.apply.countLineFlow(qingFen_name,data_grp)
    qingFen_result.show()

//    qingFen_result.map(_.mkString(",")).rdd.repartition(1)
//      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\result_2")

  }
}
case class qingFenOD(o_station_id: String,d_station_id:String,line_id:String,prob:Double)
case class qingFenOD2(o_station_id: String,d_station_id:String,line_id1:String,line_id2:String,prob:Double)
case class qingFenOD3(o_station_id: String,d_station_id:String,line_id1:String,line_id2:String,line_id3:String,prob:Double)
case class qingFenOD4(o_station_id: String,d_station_id:String,line_id1:String,line_id2:String,line_id3:String,line_id4:String,prob:Double)

case class Subway_withZone(card_id: String, o_time: String, o_line: String, o_station_name: String, o_lon: Double, o_lat: Double,
                           d_time: String, d_line: String, d_station_name: String, d_lon: Double, d_lat: Double,o_zone:String,o_street:String,
                           o_traffic:String,d_zone:String,d_Street:String,d_traffic:String)
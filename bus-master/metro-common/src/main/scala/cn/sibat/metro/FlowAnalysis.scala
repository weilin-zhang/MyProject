package cn.sibat.metro

import breeze.linalg.max
import cn.sibat.metro.utils.TimeUtils
import cn.sibat.truck._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.log4j.{Level, Logger}

import cn.sibat.metro.guotu.{qingFenOD, qingFenOD2, qingFenOD3, qingFenOD4}
import scala.io.{BufferedSource, Source}

/**
  * 对清洗后的深圳通数据进行客流统计分析
  * Created by wing1995 on 2017/5/23.
  */
class FlowAnalysis(ds: Dataset[String]) extends Serializable{
  import ds.sparkSession.implicits._
  //加载清洗后的数据，将数据格式化为DataFrame，时间解析为小时
  val formatData: DataFrame = this.ds.map(value => value.split(",")).map(x => SZTAddDate(x(1), x(2), x(3), x(4).slice(11, 13), x(5), x(6), x(8))).toDF()

  /**
    * 每一条线路每一个时段的客流量统计
    * @return flowsByPerRoutePerHour
    */
  def flowByRouteAndHour(): DataFrame = {
    val flowsByRouteAndHour = formatData.groupBy(col("routeName"), col("cardHour"), col("cardDate")).count()
    flowsByRouteAndHour
  }

  /**
    * 每一个站点每一个时段的客流量统计
    * @return flowsBySiteAndHour
    */
  def flowBySiteAndHour(): DataFrame = {
    val flowsBySiteAndHour = formatData.groupBy(col("siteName"), col("cardHour"), col("cardDate")).count()
    flowsBySiteAndHour
  }

  /**
    * 每一个站点对应不同路线不同时段的客流量统计（比如转乘站）
    * @return flowsBySiteRouteAndHour
    */
  def flowBySiteRouteAndHour(): DataFrame = {
    val flowsBySiteRouteAndHour = formatData.groupBy(col("siteName"), col("routeName"), col("cardHour"), col("cardDate")).count()
    flowsBySiteRouteAndHour
  }

  /**
    * will 添加
    * @return
    */
  def flowByAllday(): DataFrame ={
    val flowByAllday = formatData.map(row=>
      row.getString(row.fieldIndex("routeName")).filter(_ =="地铁三号线")
    ).toDF()
    flowByAllday
  }

  def id2Name(id:String,staticInfo:Dataset[Subway_Static]):String={
    val id_name = scala.collection.mutable.Map[String,String]()
    staticInfo.foreach(elem=>{
      val id = elem.station_id
      val name = elem.station_name
      id_name.put(id,name)
    })
    id_name(id)
  }

  def formatUtilQingFen(ds:Dataset[String]):DataFrame={
    import ds.sparkSession.implicits._
    val filteredDf = ds.map(_.split(",")).filter(_.length == 7)
      .map(arr => qingFenOD4(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6).toDouble)).toDF()
    filteredDf
  }

}

//去除记录编码，添加刷卡日期，并将刷卡时间转换为小时
case class SZTAddDate(cardCode: String, terminalCode: String, transType: String, cardHour: String, routeName: String, siteName: String, cardDate: String)

object FlowAnalysis {
  def apply(ds: Dataset[String]): FlowAnalysis = new FlowAnalysis(ds)

  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0\\hadoop-common-2.2.0-bin")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]") //本地模式
      .appName("flowCount")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryo.registrator", "cn.sibat.metro.MetroODRegistrator")
//      .config("spark.rdd.compress", "true")
//      .config("spark.some.config.option", "config-value")
      .getOrCreate()

/*  //代码功能：将O D 站点在同一线路（or不同线路）的过滤出来
    val path = "E:\\trafficDataAnalysis\\guotu\\testdata\\20161201_clean"
    val data = spark.read.textFile(path)

    import spark.implicits._
    val data_ds = data.map(line =>{
      val arr = line.split(",")
      OD(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6).toDouble,arr(7),arr(8),arr(9),arr(10))
      })
    print(data_ds.getClass.getSimpleName)  // 在数据或者变量后面加 .getClass.getSimpleName 就可获得其类型

    val yiHaoXian = data_ds.filter(line=>line.routeName=="地铁一号线" )
//    val yiHaoXian = data_ds.filter(line=>line.getString(line.fieldIndex("routeName"))=="地铁一号线")
    val change = data_ds.filter(col("routeName") =!= col("outRouteName")).toDF()
    val notChange = data_ds.filter(col("routeName") === col("outRouteName")).toDF()

    change.describe("cardCode","tradeValue").show()
    val arr = change.collect()  //将df 每一行转换成数组的一个元素
    print(arr(1))
    change.dtypes.foreach(println)*/


    //代码功能：根据清分来计算地铁线路客流量
    val path = "E:\\trafficDataAnalysis\\subway\\*"
    val data_source = spark.read.textFile(path)
    val path_static = "E:\\trafficDataAnalysis\\subway_station.station"
    val data_static = spark.read.textFile(path_static)

    val path_qingFen = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\workday_wan_1.txt"
    val data_qingFen = spark.read.textFile(path_qingFen)

    val path_qingFen2 = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\workday_wan_2.txt"
    val data_qingFen2 = spark.read.textFile(path_qingFen2)

    val path_qingFen3 = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\workday_wan_3.txt"
    val data_qingFen3 = spark.read.textFile(path_qingFen3)

    val path_qingFen4 = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\workday_wan_4.txt"
    val data_qingFen4 = spark.read.textFile(path_qingFen4)

    //为输入文件添加对应字段
    import spark.implicits._

    val data_ds = data_source.map(line => {
      val arr = line.split(",")
      Subway(arr(0), arr(1), arr(2), arr(3), arr(4).toDouble, arr(5).toDouble, arr(6), arr(7), arr(8),
        arr(9).toDouble, arr(10).toDouble)
    }).toDF()

    val static_ds =data_static.map(line => {
      val arr = line.split(",")
      Subway_Static(arr(0), arr(1), arr(2), arr(3))
    }).toDF()


    val qingFen_ds4 = FlowAnalysis.apply(data_qingFen4).formatUtilQingFen(data_qingFen4).toDF()

    val qingFen_ds = data_qingFen.map(line=>{
      val arr = line.split(",")
      qingFenOD(arr(0), arr(1), arr(2), arr(3).toDouble)
    }).toDF()

    val qingFen_ds2 = data_qingFen2.map(line=>{
      val arr = line.split(",")
      qingFenOD2(arr(0), arr(1), arr(2), arr(3),arr(4).toDouble)
    }).toDF()

    val qingFen_ds3 = data_qingFen3.map(line=>{
      val arr = line.split(",")
      qingFenOD3(arr(0), arr(1), arr(2), arr(3),arr(4),arr(5).toDouble)
    }).toDF()

//    val qingFen_ds4 = data_qingFen4.map(line=>{
//      val arr = line.split(",")
//      qingFenOD4(arr(0), arr(1), arr(2), arr(3),arr(4),arr(5),arr(6).toDouble)
//    }).toDF()


    /**
      * 想用join 的方法添加 name,好像行不通。。。
      *
      */
    //根据静态表将清分数据添加station_name, line_name
/*    val qingFen_O = qingFen_ds.withColumnRenamed("line_id","prob_line_id")
      .join(static_ds,qingFen_ds("o_station_id") === static_ds("station_id"),"left_outer")
      .withColumnRenamed("station_name","o_station_name")
    val qingFen_D = qingFen_O.join(static_ds,qingFen_O("d_station_id") === static_ds("station_id"))
      .withColumnRenamed("station_name","d_station_name")
      .select("o_station_id","o_station_name","d_station_id","d_station_name","prob_line_id","prob")
    qingFen_ds.show()
    qingFen_O.show()
    qingFen_D.show()*/

    /**
      * 想尝试用 DataFormat 中的getStationMap 方法 好像也行不通
      *
      */
/*        val static_df = DataFormat.apply(spark).getStationMap(path_static)
      val qingFen_name = qingFen_ds.foreach(line=>{
      val o_station_name = static_df.value.getOrElse(line.o_station_id, Array()).head._2
      val d_station_name = static_df.value.getOrElse(line.d_station_id, Array()).head._2
      val line_name = static_df.value.getOrElse(line.line_id, Array()).head._4
      (line.o_station_id,o_station_name,line.d_station_id,d_station_name,line.line_id,line.prob)
    })*/

/*    val qingFen_name = qingFen_ds.collect().map(line=>{
      val o_station_name = static_df.value.getOrElse(line.o_station_id, Array()).head._2
      val d_station_name = static_df.value.getOrElse(line.d_station_id, Array()).head._2
      val line_name = static_df.value.getOrElse(line.line_id, Array()).head._4
      (line.o_station_id,o_station_name,line.d_station_id,d_station_name,line.line_id,line_name,line.prob)
    })*/

    /**
      *采用 spark.read.textFile 读取文件，使用特例调用函数可行，但对 qingFen_ds 每行调用会报 nullPoint 错误
      */
    /*    def lineId2Name(id:String,staticInfo:DataFrame):String={
      val id_name = scala.collection.mutable.Map[String,String]()
      staticInfo.collect().foreach(elem=>{
        val id = elem.getString(elem.fieldIndex("line_id"))
        val name = elem.getString(elem.fieldIndex("line_name"))
        id_name.put(id,name)
      })
      id_name(id)
    }*/

    /**
      * 采用 Source.fromFile 的方法实现，缺点是只能读本地的文件，不能读集群的
      */
    def stationId2Name(id:String):String={
      val data = Source.fromFile(path_static)
      val id_name = scala.collection.mutable.Map[String,String]()
      data.getLines().foreach(elem=>{
        val s = elem.split(",")
        val id = s(0)
        val name = s(1)
        id_name.put(id,name)
      })
      id_name(id)
    }

    def lineId2Name(id:String):String={
      val data = Source.fromFile(path_static)
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
      * 给清分数据添加 name 字段
      */
    val qingFen_name = qingFen_ds.map(line=>{
      val o_name = stationId2Name(line.getString(0))
      val d_name = stationId2Name(line.getString(1))
      val line_name =lineId2Name(line.getString(2))
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line_name,line.getDouble(3))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line_id","line_name","prob")

    val qingFen_name2 = qingFen_ds2.map(line=>{
      val o_name = stationId2Name(line.getString(0))
      val d_name = stationId2Name(line.getString(1))
      val line1_name =lineId2Name(line.getString(2))
      val line2_name = lineId2Name(line.getString(3))
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line1_name,line.getString(3),line2_name,line.getDouble(4))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line1_id","line1_name","line2_id","line2_name","prob")

    val qingFen_name3 = qingFen_ds3.map(line=>{
      val o_name = stationId2Name(line.getString(0))
      val d_name = stationId2Name(line.getString(1))
      val line1_name =lineId2Name(line.getString(2))
      val line2_name = lineId2Name(line.getString(3))
      val line3_name = lineId2Name(line.getString(4))
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line1_name,line.getString(3),line2_name,
        line.getString(4),line3_name, line.getDouble(5))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line1_id","line1_name","line2_id","line2_name",
      "line3_id","line3_name","prob")

    val qingFen_name4 = qingFen_ds4.map(line=>{
      val o_name = stationId2Name(line.getString(0))
      val d_name = stationId2Name(line.getString(1))
      val line1_name =lineId2Name(line.getString(2))
      val line2_name = lineId2Name(line.getString(3))
      val line3_name = lineId2Name(line.getString(4))
      val line4_name = lineId2Name(line.getString(5))
      (line.getString(0),o_name,line.getString(1),d_name,line.getString(2),line1_name,line.getString(3),line2_name,
        line.getString(4),line3_name,line.getString(5),line4_name, line.getDouble(6))
    }).toDF("o_station_id","o_station_name","d_station_id","d_station_name","line1_id","line1_name","line2_id","line2_name",
      "line3_id","line3_name","line4_id","line4_name","prob")


    qingFen_name2.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("C:\\Users\\will\\Desktop\\worday_name\\wan\\workday_2")
    qingFen_name3.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("C:\\Users\\will\\Desktop\\worday_name\\wan\\workday_3")
    qingFen_name4.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("C:\\Users\\will\\Desktop\\worday_name\\wan\\workday_4")



    // 取o_time 和d_time 的中间时间作为高峰的过滤时间
    val data_format = data_ds.map(line=> {
      val Otime = line.getString(line.fieldIndex("o_time")).substring(11, 19)
      val Dtime = line.getString(line.fieldIndex("d_time")).substring(11, 19)
      val OtimeStamp = TimeUtils.apply.date2Stamp(TimeUtils.apply.time2Date(Otime, "HH:mm:ss"))
      val DtimeStamp = TimeUtils.apply.date2Stamp(TimeUtils.apply.time2Date(Dtime, "HH:mm:ss"))
      val timeMid = TimeUtils.apply.stamp2Date(OtimeStamp+(DtimeStamp-OtimeStamp)/2).toString.substring(11,19)
      (line.getString(line.fieldIndex("card_id")), timeMid, line.getString(line.fieldIndex("o_line")), line.getString(line.fieldIndex("o_station_name")),
        line.getString(line.fieldIndex("d_line")),  line.getString(line.fieldIndex("d_station_name")))
    }).toDF("card_id","timeMid","o_line","o_station_name","d_line","d_station_name")

    // 过滤出早高峰的OD数据
    val data_zao = data_format.filter(row => {
      val time_zao = row.getString(row.fieldIndex("timeMid"))
      time_zao.matches("08:.*")
    })

    // 过滤出晚高峰的OD数据
    val data_wan = data_format.filter(row => {
      val time_wan = row.getString(row.fieldIndex("timeMid"))
      time_wan.matches("18:.*")
    })

    // 过滤出非高峰时段的OD数据
    val data_other = data_format.filter(row => {
      val time = row.getString(row.fieldIndex("timeMid"))
      !(time.matches("08:.*") || time.matches("18:.*"))
    })

    val data_grp = data_wan.groupBy(col("o_station_name"),col("d_station_name")).count()

    /**
      * 清分数据分别和高峰数据进行 join,得到高峰线路客流量
      */
    val qingFen_result = qingFen_name
      .join(data_grp,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line_name").sum("num")

    val qingFen_result2 = qingFen_name2
      .join(data_grp,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line1_name","line2_name").sum("num")

    val qingFen_result3 = qingFen_name3
      .join(data_grp,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line1_name","line2_name","line3_name").sum("num")

    val qingFen_result4 = qingFen_name4
      .join(data_grp,Seq("o_station_name","d_station_name")) // 使用Seq 可以根据两个字段进行join ,并且去除了重复列
      .withColumn("num",col("count")*col("prob"))
      .groupBy("line1_name","line2_name","line3_name","line4_name").sum("num")


//    qingFen_result.map(_.mkString(",")).rdd.repartition(1)
//      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\result_1")
//    qingFen_result2.map(_.mkString(",")).rdd.repartition(1)
//          .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\result_2")
//    qingFen_result3.map(_.mkString(",")).rdd.repartition(1)
//      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\result_3")
//    qingFen_result4.map(_.mkString(",")).rdd.repartition(1)
//      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan\\result_4")


  }
}
// data_zao.dropDuplicates("o_line")  根据特定的列进行去重
//case class qingFenOD(o_station_id: String,d_station_id:String,line_id:String,prob:Double)
//case class qingFenOD2(o_station_id: String,d_station_id:String,line_id1:String,line_id2:String,prob:Double)
//case class qingFenOD3(o_station_id: String,d_station_id:String,line_id1:String,line_id2:String,line_id3:String,prob:Double)
//case class qingFenOD4(o_station_id: String,d_station_id:String,line_id1:String,line_id2:String,line_id3:String,line_id4:String,prob:Double)
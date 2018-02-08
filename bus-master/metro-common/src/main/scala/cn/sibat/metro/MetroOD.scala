package cn.sibat.metro

import cn.sibat.metro.utils.TimeUtils
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 地铁乘客OD计算
  * Created by wing1995 on 2017/5/10.
  */
class MetroOD(df: DataFrame) extends Serializable {
    import this.df.sparkSession.implicits._
  /**
    * 将清洗完的数据返回
    * @return DataFrame
    */
  def toDF: DataFrame = this.df

  /**
    * 构造伴生对象，返回对象本身，实现链式写法
    * @param df 清洗后的DataFrame
    * @return 原对象 DataClean
    */
  private def newUtils(df: DataFrame): MetroOD = new MetroOD(df)

  /**
    * 将乘客的相邻两个刷卡记录两两合并为一条字符串格式的OD记录
    *
    * @param arr 每一个乘客当天的刷卡记录组成的数组
    * @return 每一个乘客当天使用深圳通乘坐地铁产生的OD信息组成的数组
    */
  def generateOD(arr: Array[String]): Array[String] = {
    val newRecords = new ArrayBuffer[String]()
    for (i <- 1 until arr.length) {
      val emptyString = new StringBuilder()
      val OD = emptyString.append(arr(i-1)).append(',').append(arr(i)).toString()
      newRecords += OD
    }
    newRecords.toArray
  }

  /**
    * 乘客OD信息的生成
    * 根据刷卡卡号进行分组，将每位乘客的OD信息转换为数组，按刷卡时间排序，
    * 将排序好的数组转换为逗号分隔的字符串，最后将字符串两两合并生成乘客OD信息
    * @return
    */
  def calMetroOD: MetroOD = {
    import df.sparkSession.implicits._
    val mergedData = df.groupByKey(row => row.getString(row.fieldIndex("cardCode")) + ',' + row.getString(row.fieldIndex("date")))
      .flatMapGroups((_, records) => {
        val sortedArr = records.toArray.sortBy(row => row.getString(row.fieldIndex("cardTime"))).map(_.mkString(","))
        val mergedOD = generateOD(sortedArr)
        mergedOD
      })
    val filteredData = mergedData.map(_.split(",")).filter(arr => arr(2)=="21" && arr(10) == "22")
    val mergedResult = filteredData.map(arr => OD(arr(0), arr(1), arr(4), arr(5), arr(6), arr(9), arr(11).toDouble, arr(12), arr(13), arr(14), arr(15))).toDF()
    newUtils(mergedResult)
  }

  /**
    * 添加过滤条件
    * 生成进站与出站的时间差列，保留出时间差小于3小时以及出入站点不同的记录
    * @return
    */
  def filteredRule: MetroOD = {
    val timeUtils = new TimeUtils
    val timeDiffUDF = udf((startTime: String, endTime: String) => timeUtils.calTimeDiff(startTime, endTime))
    val ODsCalTimeDiff = df.withColumn("timeDiff", timeDiffUDF(col("cardTime"), col("outCardTime")))
    val timeLessThan3 = ODsCalTimeDiff.filter(col("timeDiff") < 3 * 60)
    val inNotEqualToOut = timeLessThan3.filter(col("siteName") =!= col("outSiteName"))
    newUtils(inNotEqualToOut)
  }

    /**
      * 给站点名称添加经纬度（先给地铁处理）
      * @param dataStation 带有站点经纬度信息的静态站点数据
      * @return
      */
  def addLocation(dataStation: Broadcast[Map[String, Array[(String, String, String, String)]]]): MetroOD = {
      val dataWithLocation = this.df.map(row => {
          val inSiteId = row.getString(row.fieldIndex("terminalCode")).substring(0, 6)
          val inSiteLon = dataStation.value.getOrElse(inSiteId, Array()).head._3.toDouble
          val inSiteLat = dataStation.value.getOrElse(inSiteId, Array()).head._4.toDouble

          val outSiteId = row.getString(row.fieldIndex("outTerminalCode")).substring(0, 6)
          val outSiteLon = dataStation.value.getOrElse(outSiteId, Array()).head._3.toDouble
          val outSiteLat = dataStation.value.getOrElse(outSiteId, Array()).head._4.toDouble

          ODWithLocation(row.getString(row.fieldIndex("cardCode")), inSiteLon, inSiteLat, outSiteLon, outSiteLat, row.getString(row.fieldIndex("cardTime")), row.getString(row.fieldIndex("outCardTime")))
      }).toDF()
      newUtils(dataWithLocation)
    }
}

/**
  * 注册类，实现数据的kryo序列化，数据减少后相对传统的java序列化后的数据大小减少3-5倍
  */
class MetroODRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[SztRecord])
        kryo.register(classOf[OD])
        kryo.register(classOf[ODWithLocation])
    }
}

object MetroOD {
    def apply(df: DataFrame): MetroOD = new MetroOD(df)

    def main(args: Array[String]): Unit = {
        var oldDataPath = "E:\\trafficDataAnalysis\\guotu\\testdata\\20161201_10000"
        var newDataPath = "E:\\trafficDataAnalysis\\guotu\\testdata\\20161201_10000"
        var staticMetroPath = "E:\\trafficDataAnalysis\\guotu\\testdata\\subway_zdbm_station"
        if (args.length == 3) {
          oldDataPath = args(0)
          newDataPath = args(1)
          staticMetroPath = args(2)
        } else if (args.length == 2) {
            newDataPath = args(0)
            staticMetroPath = args(1)
        } else System.out.println("使用默认的参数配置")

        val spark = SparkSession.builder()
            .appName("metroOD")
            .master("local[3]")
//            .config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "cn.sibat.metro.MetroODRegistrator")
            .config("spark.rdd.compress", "true")
            .getOrCreate()

        val BMUsers = Array(
            "086540035", "665388436", "293345165", "327491360", "323555676",
            "331357991", "684666554", "023041813", "328771992", "362774134",
            "292335926", "660941532", "684043989", "361823600", "687307709",
            "362756265", "667338104", "685844167", "362166709", "295587058",
            "329813505", "684160474", "322193400", "684993919"
        )
        import spark.implicits._

        val oldDf = DataFormat.apply(spark).getOldData(oldDataPath)
        val newDf = DataFormat.apply(spark)
            .getNewData(newDataPath)
            .filter(row => {
                val siteId = row.getString(row.fieldIndex("terminalCode")).substring(0, 6)
                val cardCode = row.getString(row.fieldIndex("cardCode"))
                BMUsers.contains(cardCode) && siteId.matches("2[46].*")
             })
        val dataStation = DataFormat.apply(spark).getStationMap(staticMetroPath)
        val bStationMap = DataFormat.apply(spark).getStationMap(staticMetroPath)
//        val df = oldDf.union(newDf).distinct()
        val cleanDf = DataClean.apply(oldDf).addDate().recoveryData(bStationMap).toDF
        val resultDf = MetroOD.apply(cleanDf).calMetroOD.filteredRule.addLocation(dataStation).toDF
//        resultDf.show()
        resultDf.map(_.mkString(",")).rdd.repartition(1).saveAsTextFile("BWUsers")

        spark.stop()
    }
}

/**
  * OD记录的基本数据结构
  * @param cardCode 刷卡卡号
  * @param cardTime 刷卡时间
  * @param terminalCode 终端编码
  * @param routeName 路径名称
  * @param siteName 站点名称
  * @param outCardTime 出站刷卡时间
  * @param tradeValue 旅程交易值
  * @param outTerminalCode 出站终端编码
  * @param outRouteName 出站线路名称
  * @param outSiteName 出站站点名称
  * @param date 日期
  */
case class OD(cardCode: String, cardTime: String, terminalCode: String, routeName: String, siteName: String,
              outCardTime: String, tradeValue: Double, outTerminalCode: String, outRouteName: String, outSiteName: String, date: String
             )

/**
  * 带经纬度的深圳通OD出行
  * @param cardCode 卡号
  * @param inSiteLon 进站站点经度
  * @param inSiteLat 进站站点纬度
  * @param outSiteLon 出站站点经度
  * @param outSiteLat 出站站点纬度
  * @param inTime 进站时间
  * @param outTime 出站时间
  */
case class ODWithLocation(cardCode: String, inSiteLon: Double, inSiteLat: Double, outSiteLon: Double, outSiteLat: Double, inTime: String, outTime: String)

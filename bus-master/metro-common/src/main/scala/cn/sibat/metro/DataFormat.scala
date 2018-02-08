package cn.sibat.metro

import cn.sibat.metro.utils.TimeUtils
import cn.sibat.truck.Subway_Static
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql._

/**
  * 注册类，实现数据的kryo序列化，数据减少后相对传统的java序列化后的数据大小减少3-5倍
  */
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SztRecord])
  }
}

/**
  * DataSet => Scheme
  * spark.read.text读进来的则是Dataset数据结构，每一行都是一个元素，类型为Row，默认只有一个value列
  * 将默认数据类型转换为业务所需的数据视图，筛选需要的字段，以及数据类型格式化
  * Created by wing1995 on 2017/5/4.
  */
class DataFormat(spark: SparkSession) {
  import spark.implicits._
  /**
    * 获取旧数据，并解析数据成标准格式
    * @param oldDataPath 旧数据地址
    * @return oldDf
    */
  def getOldData(oldDataPath: String): DataFrame = {
    import spark.implicits._
    //读取深圳通旧数据（—2017.05.06）
    val oldData = spark.read.textFile(oldDataPath)
    val oldDf = oldData.map(_.split(","))
      .filter(recordArr => recordArr.length == 15)
      .map(recordArr => {
        val cardCode = recordArr(1)
        val cardTime = recordArr(8)
        val tradeType = recordArr(4)
        val trulyTradeValue = recordArr(6).toDouble / 100
        //实际交易值
        val terminalCode = recordArr(2)
        val routeName = recordArr(12)
        val siteName = recordArr(13)
        SztRecord(cardCode, cardTime, tradeType, trulyTradeValue, terminalCode, routeName, siteName)
      }).toDF()
    oldDf
  }

  /**
    * 获取新数据
    * @param newDataPath 新数据地址
    * @return newDf
    */
  def getNewData(newDataPath: String): DataFrame = {
    //读取GBK编码的csv文件，新数据（2017.05.07—）
    val newData = spark.read
      .format("csv")
      .option("encoding", "GB2312")
      .option("nullValue", "None")
      .csv(newDataPath)

    val header = newData.first()

    val newDf = newData.filter(_ != header)
      .map(row => {
        val cardCode = row.getString(0)
        val cardTime = TimeUtils.apply.stamp2time(TimeUtils.apply.time2stamp(row.getString(1), "yyyyMMddHHmmss"), "yyyy-MM-dd HH:mm:ss")
        var tradeType = row.getString(2)
        tradeType = tradeType match {
          case "地铁入站"  => "21"
          case "地铁出站"  => "22"
          case "巴士"  => "31"
        }
        val trulyTradeValue = row.getString(3).toDouble / 100
        //实际交易值
        val terminalCode = row.getString(5)
        val routeName = row.getString(6)
        val siteName = row.getString(7)
        SztRecord(cardCode, cardTime, tradeType, trulyTradeValue, terminalCode, routeName, siteName)
      }).toDF()

    newDf
  }

  /**
    * 获取站点ID对应站点名称的map，并广播到小表到各个节点
    *
    * @param staticMetroPath 静态表路径
    * @return bStationMap 以站点ID为key的站点名称，经纬度信息
    */
  def getStationMap(staticMetroPath: String): Broadcast[Map[String, Array[(String, String, String, String)]]] = {
    import spark.implicits._
    val ds = spark.read.textFile(staticMetroPath)
    val stationMap = ds.map(line => {
      //System.out.println(line)
      val lineArr = line.split(",")
      (lineArr(0), lineArr(1), lineArr(5), lineArr(4))
    })
      .collect()
      .groupBy(row => row._1)

    val bStationMap = spark.sparkContext.broadcast(stationMap)
    bStationMap
  }

}

/**
  * 深圳通卡常规记录
  *
  * @param cardCode        卡号
  * @param cardTime        刷卡时间
  * @param tradeType       交易类型
  * @param trulyTradeValue 真实交易金额
  * @param terminalCode    终端编码
  * @param routeName       线路名称
  * @param siteName        站点名称
  */
case class SztRecord(cardCode: String, cardTime: String, tradeType: String, trulyTradeValue: Double,
                     terminalCode: String, routeName: String, siteName: String)

object DataFormat{
  def apply(spark: SparkSession): DataFormat = new DataFormat(spark)
}

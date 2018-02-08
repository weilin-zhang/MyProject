package cn.sibat.metro.metroFlowForcast

import cn.sibat.metro.utils.TimeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
//import cn.ODBackModel.utils.NoNameExUtil

/**
  * 计算每一天各站点的客流分布情况
  * @param ODTimeData 所有站点的OD出行平均时间数据
  * Created by wing1995 on 2017/7/31.
  */
class BigTraffic(ODTimeData: DataFrame) {
  /**
    * 为每个乘客的OD记录生成新的出站时间
    * @param ODRecordData 乘客OD出行记录
    * @return
    */
  def generateNewTime(ODRecordData: DataFrame): DataFrame = {
    val joinedData = ODRecordData.join(ODTimeData, Seq("outSiteCode", "inSiteCode"))
    val getNewOutCardTime = udf{(inCardTime: String, avgTime: String) => {
      val inCardTimeStamp =  TimeUtils.apply.time2stamp(inCardTime, "yyyy-MM-dd'T'HH:mm:ss.SSS")
      val outCardTimeStamp = inCardTimeStamp + avgTime.toDouble * 60
      TimeUtils.apply.stamp2time(outCardTimeStamp.toLong, "yyyy-MM-dd'T'HH:mm:ss.SSS")
    }}
    val newRecords = joinedData.withColumn("newOutCardTime", getNewOutCardTime(col("inCardTime"), col("avgTime")))
    newRecords
  }
}

object BigTraffic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Metro Data Test")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val ODTimeDs = spark.read.textFile(args(0)) //读取静态站点OD花费时间数据
    val recordDs = spark.read.textFile(args(1)) //读取历史地铁深圳通记录
    val ODTimeDf = ODTimeDs.map(_.split(",")).map(row => ODTime(row(0), row(1), row(2))).toDF()
    val recordDf = recordDs.map(_.split(",")).map(arr => Metro(arr(0), arr(1), arr(2), arr(3), arr(4))).toDF()
    var ODRecordDf = StationTime.apply(recordDf).cleanData().addDate().getDF.filter(col("date") === "2017-01-02")
    ODRecordDf = StationTime.apply(ODRecordDf).mergeOD().getDF
    val result = BigTraffic.apply(ODRecordDf).generateNewTime(ODTimeDf).select(col("outSiteCode"), col("newOutCardTime").as("outSiteTime"))
    val getHourOfTime = udf((outSiteTime: String) => outSiteTime.substring(11, 13))
//    val code2name = udf((outSiteCode: String) => NoNameExUtil.NO2Name(outSiteCode))
//    result.withColumn("outSiteName", code2name(col("outSiteCode"))).withColumn("outSiteHour", getHourOfTime(col("outSiteTime"))).groupBy(col("outSiteName"), col("outSiteHour")).count().orderBy(desc("count")).repartition(1).write.csv("E:\\trafficDataAnalysis\\BigTrafficData\\20170102")
  }
  def apply(data: DataFrame): BigTraffic = new BigTraffic(data)
}

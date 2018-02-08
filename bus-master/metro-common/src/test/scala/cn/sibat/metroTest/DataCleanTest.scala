package cn.sibat.metroTest

import cn.sibat.metro.{DataClean, DataFormat}
import org.apache.spark.sql.SparkSession

/**
  * 测试数据输出情况
  * Created by wing1995 on 2017/4/20
  */
object DataCleanTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]") // 只能通过打包的方式运行java/scala代码，本身spark的设计原因，还有就是通过pycharm直接连接到远程环境也可以
      .getOrCreate()

      import spark.implicits._
    //读取原始数据，格式化
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\testData\\oldTest_20170102") //原始数据
    val ds_station = spark.read.textFile("E:\\trafficDataAnalysis\\subway_station") //站点静态表
    val df_metro = DataFormat(spark).getOldData("E:\\trafficDataAnalysis\\testData\\oldTest_20170102")
    val stationMap = ds.map(line => {
      //System.out.println(line)
      val lineArr = line.split(",")
      (lineArr(0), lineArr(1))})
      .collect()
      .groupBy(row => row._1)
      .map(grouped => (grouped._1, grouped._2.head._2))

    val bStationMap = spark.sparkContext.broadcast(stationMap)

    //执行时间重新划分和数据恢复得到最终的清洗数据
    val df_clean = new DataClean(df_metro).addDate().toDF
    println(df_clean.count())
    //df_clean.rdd.map(x => x.mkString(",")).repartition(1).saveAsTextFile("E:\\trafficDataAnalysis\\cleanData\\2017-01-02")
//    //测试SZT打卡时间分布
//    val colHour = udf {(cardTime: String) => cardTime.slice(11, 13)}
//    val df_SZT = DataFormatUtils.apply.trans_SZT(ds)
//    df_metro.withColumn("hour", colHour(col("cardTime"))).filter(col("hour") === "04").select("cardCode", "hour")
//      .join(df_metro, Seq("cardCode"), "inner")
//      .select("cardCode", "hour", "recordCode", "terminalCode", "transType", "cardTime", "routeName", "siteName", "GateMark")
//      .show()

  }
}
//20170101数据计数（以供参考）
//4519343 result_subway clean_subway 4462810
//698447 to_recovery
//698447 missing
//4519343 subway
//7358615 all
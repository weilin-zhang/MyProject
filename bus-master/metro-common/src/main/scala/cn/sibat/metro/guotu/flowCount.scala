package cn.sibat.metro.guotu


import cn.sibat.metro._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
//import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class flowCount(ds:Dataset[String]) extends Serializable{
  import ds.sparkSession.implicits._
  val formatData: DataFrame = this.ds.map(value => value.split(",")).map(line => SZTAddDate(line(1), line(2), line(3),
    line(4).slice(11, 13), line(5), line(6), line(8))).toDF()

  /**
    * 过滤出地铁三号线的SZT 数据
    * @return
    */
  def filterByline(): DataFrame ={
    val filterByline = formatData.map(row=>
      row.getString(row.fieldIndex("routeName")).filter(_ =="地铁三号线")
    ).toDF()
    filterByline
  }
}

object flowCount {

//  Logger.getLogger("org").setLevel(Level.ERROR)
//  System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0\\hadoop-common-2.2.0-bin")

  def apply(ds: Dataset[String]): flowCount = new flowCount(ds)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]") //本地模式，启用5个线程
      .appName("flowCount")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "cn.sibat.metro.MetroODRegistrator")
      .config("spark.rdd.compress", "true")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    import spark.implicits._

    val oldData = "data/20161201_10000"   // "E:\\trafficDataAnalysis\\guotu\\testdata\\20161201_10000 "
    val staticMetroPath = "data/subway_zdbm_station"  //  "E:\\trafficDataAnalysis\\guotu\\testdata\\subway_zdbm_station"
    val data_df = DataFormat.apply(spark).getOldData(oldData)
    val static_df = DataFormat.apply(spark).getStationMap(staticMetroPath) // .value 值对应的stationID，stationName,lon,lat
  /*  val dataStation = DataFormat.apply(spark).getStationMap(staticMetroPath)
    val result = static_df.value.getOrElse("260017", Array()).head._2   //取出stationID为260017 对应的 value值，如果没有 则返回空数组, .head._2表示取出第二个元素
    print(result)*/

    val cleanDf = DataClean.apply(data_df).addDate().recoveryData(static_df).toDF
    val resultDf = MetroOD.apply(cleanDf).calMetroOD.filteredRule.toDF   //filteredRule.addLocation(static_df).toDF  获取进出站的经纬度

    val resultDFs = resultDf.toDF("cardCode": String, "cardTime": String, "terminalCode": String, "routeName": String, "siteName": String,
      "outCardTime": String, "tradeValue": String, "outTerminalCode": String, "outRouteName": String, "outSiteName": String, "date": String,"timeDiff":String)

    val yiHaoXian = resultDFs.filter(line=>line.getString(line.fieldIndex("routeName"))=="地铁一号线")
    yiHaoXian.show()
    val notChange = resultDFs.filter(col("routeName") === col("outRouteName")).toDF()
    col("routeName") ==="地铁一号线"
    spark.stop()

  }
}







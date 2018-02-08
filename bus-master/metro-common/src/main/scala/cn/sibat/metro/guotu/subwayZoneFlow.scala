package cn.sibat.metro.guotu

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object subwayZoneFlow {
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
    val path = "E:\\trafficDataAnalysis\\subway_withZone\\2016-12-01"
    val data_source = spark.read.textFile(path)
    val path_static = "E:\\trafficDataAnalysis\\subway_station.station"
    val path_qingFen = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\wan"
    val data_qingFen = spark.read.textFile(path_qingFen)

    val data_ds = subwayFlow.apply.formatUtilSubway_withZone(data_source)

    /**
      * 为清分数据添加name 字段，以仅换乘一次为例，
      * 换乘 n 次的只需更改formatUtilQingFen2() ,addNameField2() 方法
      */
    val qingFen_ds = subwayFlow.apply.formatUtilQingFen(data_qingFen).toDF()
    val qingFen_name = subwayFlow.apply.addNameField(qingFen_ds, path_static)

    val qingFen_ds2 = subwayFlow.apply.formatUtilQingFen2(data_qingFen).toDF()
    val qingFen_name2 = subwayFlow.apply.addNameField2(qingFen_ds2, path_static)

    val qingFen_ds3 = subwayFlow.apply.formatUtilQingFen3(data_qingFen).toDF()
    val qingFen_name3 = subwayFlow.apply.addNameField3(qingFen_ds3, path_static)

    val qingFen_ds4 = subwayFlow.apply.formatUtilQingFen4(data_qingFen).toDF()
    val qingFen_name4 = subwayFlow.apply.addNameField4(qingFen_ds4, path_static)
    //    qingFen_name.map(_.mkString(",")).rdd.repartition(1)
    //      .saveAsTextFile("C:\\Users\\will\\Desktop\\worday_name\\wan\\workday_2")


    val data_format = subwayFlow.apply.addTimeMid(data_ds)
    /**
      * 以晚高峰为例,得到换乘一次的线路客流量
      * 其他时间段只需更改 filterWan()方法
      * 要得到换乘 n 次的要更改 countLineFlow() 方法中的 groupBy("line1_name","line2_name",.....)
      * 例如不换乘的 groupBy("line_name")
      */
    val data_wan = subwayFlow.apply.filterWan(data_format)
    val data_grp = subwayFlow.apply.countOD(data_wan)
    val qingFen_result = subwayFlow.apply.countLineFlow(qingFen_name, data_grp)
    val qingFen_result2 = subwayFlow.apply.countLineFlow2(qingFen_name2, data_grp)
    val qingFen_result3= subwayFlow.apply.countLineFlow3(qingFen_name3, data_grp)
    val qingFen_result4 = subwayFlow.apply.countLineFlow4(qingFen_name4, data_grp)
    qingFen_result.show()

  }
}

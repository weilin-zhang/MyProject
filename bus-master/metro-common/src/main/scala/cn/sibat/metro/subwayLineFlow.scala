package cn.sibat.metro

import cn.sibat.metro.utils.TimeUtils
import cn.sibat.truck._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.log4j.{Level, Logger}

object subwayLineFlow {

  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0\\hadoop-common-2.2.0-bin")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("subwayLineFlow")
      .getOrCreate()


    val qingFen_path2 = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_2"
    val data_qingFen2 = spark.read.textFile(qingFen_path2)
    val qingFen_path3 = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_3"
    val data_qingFen3 = spark.read.textFile(qingFen_path3)
    val qingFen_path4 = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_4"
    val data_qingFen4 = spark.read.textFile(qingFen_path4)

    import spark.implicits._
    val result2_ds = data_qingFen2.map(line => {
      val arr = line.split(",")
      result2(arr(0), arr(1), arr(2).toDouble)
    }).toDF()
    val result3_ds = data_qingFen3.map(line => {
      val arr = line.split(",")
      result3(arr(0), arr(1), arr(2),arr(3).toDouble)
    }).toDF()
    val result4_ds = data_qingFen4.map(line => {
      val arr = line.split(",")
      result4(arr(0), arr(1), arr(2),arr(3),arr(4).toDouble)
    }).toDF()

    val data_grp2_1 = result2_ds.groupBy("line1_name").sum("num")
    val data_grp2_2 = result2_ds.groupBy("line2_name").sum("num")
    data_grp2_1.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_2_1")
    data_grp2_2.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_2_2")


    val data_grp3_1 = result3_ds.groupBy("line1_name").sum("num")
    val data_grp3_2 = result3_ds.groupBy("line2_name").sum("num")
    val data_grp3_3 = result3_ds.groupBy("line3_name").sum("num")
    data_grp3_1.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_3_1")
    data_grp3_2.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_3_2")
    data_grp3_3.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_3_3")

    val data_grp4_1 = result4_ds.groupBy("line1_name").sum("num")
    val data_grp4_2 = result4_ds.groupBy("line2_name").sum("num")
    val data_grp4_3 = result4_ds.groupBy("line3_name").sum("num")
    val data_grp4_4 = result4_ds.groupBy("line4_name").sum("num")
    data_grp4_1.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_4_1")
    data_grp4_2.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_4_2")
    data_grp4_3.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_4_3")
    data_grp4_4.map(_.mkString(",")).rdd.repartition(1)
      .saveAsTextFile("E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\ping\\result_4_4")


  }
}

case class result2(line1_name: String,line2_name:String,num:Double)
case class result3(line1_name: String,line2_name:String,line3_name:String,num:Double)
case class result4(line1_name: String,line2_name:String,line3_name:String,line4_name:String,num:Double)
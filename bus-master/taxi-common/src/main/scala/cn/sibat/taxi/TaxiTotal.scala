package cn.sibat.taxi

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/7/1.
  */
object TaxiTotal {

  def taxiTotal(spark: SparkSession, argDate: String): Unit = {

    import spark.implicits._

//        val list = "20160913\n20160920\n20160927\n20161004\n20161018\n20161025\n20161101\n20161108\n20161129\n20161206\n20161213\n20161220\n20161227\n20170103\n20170110\n20170117\n20170124\n20170207\n20170214\n20170221\n20170228\n20170307\n20170321\n20170328\n20170411\n20170418\n20170425\n20170502\n20170509\n20170516\n20170523\n20170530\n20170606\n20170613\n20170620\n20170627".split("\n")
//        var min = list(0)
//        var minNum = Int.MaxValue
//        list.foreach(s =>
//          if (minNum > s.toInt - argDate.replaceAll("_", "").toInt) {
//            min = s
//            minNum = s.toInt - argDate.replaceAll("_", "").toInt
//          }
//        )
//
//        //加载出租车类型表
//        ///user/kongshaohong/taxiStatic/
//        //D:/testData/公交处/taxiStatic/
//        val carStaticDate = spark.read.textFile("D:/testData/公交处/data/taxiStatic/" + min).map(str => {
//          val Array(carId, color, company) = str.split(",")
//          TaxiStatic(carId, color, company)
//        })
    //      .collect()
    //    val bCarStaticDate = spark.sparkContext.broadcast(carStaticDate)

//        val data2 = spark.read.textFile("D:/testData/公交处/data/TAXIMETERS_DEAL_" + argDate + "*").map(
//          line => line.replaceAll(",,", ",null,")
//        ).distinct()
//
//        val TaxiDealCleanUtils = new TaxiDealCleanUtils(data2.toDF())
//        val taxiDealClean = TaxiDealCleanUtils.dataFormat().distinct().ISOFormatRe().getField(carStaticDate)

        val sdf1 = new SimpleDateFormat("yyyy/M/d H:mm:ss")
        val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val taxiDealClean = spark.read.option("header", true).option("inferSchema", true).csv("D:/testData/公交处/data/taxiDeal/P_JJQ_201609*.csv")
          .toDF("id","termId", "carId", "upTime", "downTime", "singlePrice", "runningDistance", "runTime", "sumPrice", "emptyDistance", "name", "carType", "aDate").map(s => {

          try {
            //      val Array(id,termId,carId,uptime,downtime,singlePrice,runningDistance,runTime,sumPrice,
            //      emptyDistance,name,carType,aDate) = s.split(",")
            val uptime = s.getString(s.fieldIndex("upTime"))
            val downtime = s.getString(s.fieldIndex("upTime"))
            val carId = s.getString(s.fieldIndex("carId"))
            val singlePrice = s.getDouble(s.fieldIndex("singlePrice"))
            val runningDistance = s.getDouble(s.fieldIndex("runningDistance"))
            val sumPrice = s.getDouble(s.fieldIndex("sumPrice"))
            val emptyDistance = s.getDouble(s.fieldIndex("emptyDistance"))
            val runTime = s.getString(s.fieldIndex("runTime"))
            val carType = s.getInt(s.fieldIndex("carType"))
            val upTime = sdf2.format(sdf1.parse(uptime))
            val downTime = sdf2.format(sdf1.parse(downtime))

            //      val Array(id, termId, carId, upTime, downTime, singlePrice, runningDistance, runTime, sumPrice,
            //      emptyDistance, name, carType, aDate) = s.split(",")

            //      val date = upTime.split(" ")(0)
            val date = upTime.split(" ")(0)

            val company = carType.toString

            val color = company match {
              case "1" => "红的"
              case "2" => "绿的"
              case "3" => "蓝的"
            }

            TaxiDealClean(carId, date, upTime, downTime, singlePrice, runningDistance, runTime, sumPrice, emptyDistance, color, company)
          } catch {
            case e: Exception => TaxiDealClean("1", "1", "1", "1", 1.1, 1.1, "1", 1.1, 1.1, "1", "1")
          }
        }).filter(s => !s.carId.equals("1"))
        taxiDealClean.write.parquet("D:/testData/公交处/taxiDeal/" + argDate)
//    val time2DateAndHour = udf((downTime: String) => {
//      val split = downTime.split(" ")
//      split(0) + "," + split(1).split(":")(0)
//    })

//    val taxiDealClean = spark.read.parquet("D:/testData/公交处/taxiDeal/" + argDate)
//
//    val group = taxiDealClean.map(row => {
//        var color = row.getString(row.fieldIndex("color"))
//        val company = row.getString(row.fieldIndex("company"))
//        if (color.equals("null")){
//          color = company match {
//            case "1" => "红的"
//            case "2" => "绿的"
//            case "3" => "蓝的"
//          }
//        }
//        TaxiDealClean(row.getString(row.fieldIndex("carId")), row.getString(row.fieldIndex("date")), row.getString(row.fieldIndex("upTime")), row.getString(row.fieldIndex("downTime")),
//          row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
//          row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), color, row.getString(row.fieldIndex("company")))
//      }).toDF().groupBy(col("color"), col("date"))
//    group.count().repartition(1).rdd.saveAsTextFile("D:/testData/公交处/taxiStat/passengerTotal/" + argDate)
//    group.sum("emptyDistance", "runningDistance").repartition(1).rdd.saveAsTextFile("D:/testData/公交处/taxiStat/disTotal/" + argDate)
//    //taxiDealClean.toDF().groupBy(col("color"), time2DateAndHour(col("downTime"))).avg("sumPrice").show()
//    taxiDealClean.map(row => {
//      var color = row.getString(row.fieldIndex("color"))
//      val company = row.getString(row.fieldIndex("company"))
//      if (color.equals("null")){
//        color = company match {
//          case "1" => "红的"
//          case "2" => "绿的"
//          case "3" => "蓝的"
//        }
//      }
//      TaxiDealClean(row.getString(row.fieldIndex("carId")), row.getString(row.fieldIndex("date")), row.getString(row.fieldIndex("upTime")), row.getString(row.fieldIndex("downTime")),
//        row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
//        row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), color, row.getString(row.fieldIndex("company")))
//    }).toDF().groupByKey(tdc => tdc.getString(tdc.fieldIndex("color")) + "," + tdc.getString(tdc.fieldIndex("date"))).flatMapGroups { (s, it) =>
//      val map = new mutable.HashMap[String, TaxiODTest]()
//      val result = new ArrayBuffer[String]()
//      val color = s.split(",")(0)
//      it.foreach(tdc => {
//        val key = tdc.getString(tdc.fieldIndex("upTime")).split(" ")(1).split(":")(0)
//        val tot = map.getOrElse(key, new TaxiODTest(0.0, 0.0, 0.0, new mutable.HashSet[String](), color))
//        val emptyDis = tot.emptyTotal + tdc.getDouble(tdc.fieldIndex("emptyDistance"))
//        val runDis = tot.runTotal + tdc.getDouble(tdc.fieldIndex("runningDistance"))
//        val priceTotal = tot.priceTotal + tdc.getDouble(tdc.fieldIndex("sumPrice"))
//        tot.set += tdc.getString(tdc.fieldIndex("carId"))
//        map.put(key, tot.copy(emptyTotal = emptyDis, runTotal = runDis, priceTotal = priceTotal, set = tot.set))
//      })
//      var tot = new TaxiODTest(0.0, 0.0, 0.0, new mutable.HashSet[String](), color)
//      map.foreach(tuple => {
//        tot = tot.copy(emptyTotal = tot.emptyTotal + tuple._2.emptyTotal, runTotal = tot.runTotal + tuple._2.runTotal
//          , priceTotal = tot.priceTotal + tuple._2.priceTotal, set = tot.set.++=(tuple._2.set))
//        result += s.split(",")(1) + "," + tuple._1 + "," + tuple._2.toString
//      })
//      result += s.split(",")(1) + ",all," + tot.toString
//      result
//    }.repartition(20).rdd.saveAsTextFile("D:/testData/公交处/taxiStat/all/" + argDate)

//    taxiDealClean.filter(col("date") === lit(argDate.replaceAll("_", "-"))).groupByKey(tdc => tdc.getString(tdc.fieldIndex("color")) + "," + tdc.getString(tdc.fieldIndex("date"))).flatMapGroups { (s, it) =>
//      val map = new mutable.HashMap[String, TaxiODTest]()
//      val result = new ArrayBuffer[String]()
//      val color = s.split(",")(0)
//      it.foreach(tdc => {
//        val key = tdc.getString(tdc.fieldIndex("upTime")).split("T")(1).split(":")(0)
//        val tot = map.getOrElse(key, new TaxiODTest(0.0, 0.0, 0.0, new mutable.HashSet[String](), color))
//        val emptyDis = tot.emptyTotal + tdc.getDouble(tdc.fieldIndex("emptyDistance"))
//        val runDis = tot.runTotal + tdc.getDouble(tdc.fieldIndex("runningDistance"))
//        val priceTotal = tot.priceTotal + tdc.getDouble(tdc.fieldIndex("sumPrice"))
//        tot.set += tdc.getString(tdc.fieldIndex("carId"))
//        map.put(key, tot.copy(emptyTotal = emptyDis, runTotal = runDis, priceTotal = priceTotal, set = tot.set))
//      })
//      var tot = new TaxiODTest(0.0, 0.0, 0.0, new mutable.HashSet[String](), color)
//      map.foreach(tuple => {
//        tot = tot.copy(emptyTotal = tot.emptyTotal + tuple._2.emptyTotal, runTotal = tot.runTotal + tuple._2.runTotal
//          , priceTotal = tot.priceTotal + tuple._2.priceTotal, set = tot.set.++=(tuple._2.set))
//        result += s.split(",")(1) + "," + tuple._1 + "," + tuple._2.toString
//      })
//      result += s.split(",")(1) + ",all," + tot.toString
//      result
//    }.repartition(1).rdd.saveAsTextFile("D:/testData/公交处/taxiStat/all/" + argDate)

  }

  def taxiStat(spark:SparkSession): Unit ={
    import spark.implicits._
    val l =spark.read.parquet("D:/testData/公交处/taxiDeal/*/*").map(row =>{
      val uptime = row.getString(row.fieldIndex("upTime"))
      val downtime = row.getString(row.fieldIndex("downTime"))
      var color = row.getString(row.fieldIndex("color"))
      val company = row.getString(row.fieldIndex("company"))
      val date = row.getString(row.fieldIndex("date")).replaceAll("/","-")
      if (color.equals("null")) {
        color = company match {
          case "1" => "红的"
          case "2" => "绿的"
          case "3" => "蓝的"
        }
      }
      TaxiDealClean(row.getString(row.fieldIndex("carId")), date, uptime,downtime,
        row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
        row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), color, company)
    }).toDF().groupBy("date","carId").count()
    l.sort(col("count").desc).show(500)
  }

  def taxiODStat(spark:SparkSession): Unit ={
    println(spark.read.textFile("D:/testData/公交处/data/2016_10_16").count())
    println(spark.read.textFile("D:/testData/公交处/data/2016_10_17").count())
    println(spark.read.textFile("D:/testData/公交处/data/2016_11_07").count())
    println(spark.read.textFile("D:/testData/公交处/data/2016_11_13").count())
    println(spark.read.textFile("D:/testData/公交处/data/2016_12_11").count())
    println(spark.read.textFile("D:/testData/公交处/data/2016_12_12").count())
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TaxiTotal").master("local[*]").getOrCreate()

//    val start = "2016-07-01"
//    val end = "2016-08-09"
//
////    ReadTaxiGPSToHDFS.getBetweenDate(start, end, "yyyy-MM-dd")
////      .map(s => s.replaceAll("-", "_")).foreach(s => taxiTotal(spark, s))
//    //taxiTotal(spark, "2016_12_00")
//    taxiTotal(spark,"2016_09_00")
    taxiODStat(spark)
  }
}

package cn.sibat.bus

import java.text.DecimalFormat

import cn.sibat.bus.utils.{DateUtil, LocationUtil}
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.special.Erf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.json4s.jackson.Json

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/7/18.
  */
object BaoMa {

  def getAddress(): Unit = {
    val url = "http://api.map.baidu.com/geocoder/v2/?coordtype=wgs84ll&output=json&location=39.983424,116.322987&ak=ynXCZcgzG6LFTNhTr5e5KVy3TbyBoAYZ"
    url.getBytes()
  }

  /**
    * 俩向量之间的欧式距离
    *
    * @param array1 向量1
    * @param array2 向量2
    * @return 距离
    */
  def distance(array1: Array[Double], array2: Array[Double]): Double = {
    var total = 0.0
    for (i <- array1.indices) {
      total += math.pow(array1(i) - array2(i), 2)
    }
    math.sqrt(total)
  }

  /**
    * 俩向量之间的欧式距离
    *
    * @param str1 向量1
    * @param str2 向量2
    * @return 距离
    */
  def distance(str1: String, str2: String): (Double, Double) = {
    val Array(o1, d1) = str1.split(";")
    val Array(o2, d2) = str2.split(";")
    val Array(o1Lon, o1Lat) = o1.split(",").map(_.toDouble)
    val Array(d1Lon, d1Lat) = d1.split(",").map(_.toDouble)
    val Array(o2Lon, o2Lat) = o2.split(",").map(_.toDouble)
    val Array(d2Lon, d2Lat) = d2.split(",").map(_.toDouble)
    val oDis = LocationUtil.distance(o1Lon, o1Lat, o2Lon, o2Lat)
    val dDis = LocationUtil.distance(d1Lon, d1Lat, d2Lon, d2Lat)
    (oDis, dDis)
  }

  def probability(x: Double, mean: Double, variance: Double): Double = {
    0.5 * (1 + Erf.erf(x - mean / (math.sqrt(variance) * math.sqrt(2.0))))
  }

  def timeMean(x: Array[Double]): (Double, Double) = {
    var mean = 0.0
    var temp = Double.MaxValue
    for (i <- 0 until 240) {
      var sum = 0.0
      for (j <- x.indices) {
        sum += math.pow(12 - math.abs(math.abs(i.toDouble / 10 - x(j)) - 12), 2)
      }
      if (sum < temp) {
        temp = sum
        mean = i.toDouble / 10
      }
    }
    (mean, temp / x.length)
  }

  def main(args: Array[String]) {
            val spark = SparkSession.builder().master("local[*]").appName("BaoMa").getOrCreate()
            import spark.implicits._
    //    val list = Array("086540035", "665388436", "023041813", "362774134", "392335926", "660941532", "684043989", "361823600"
    //      , "667338104", "685844167", "362166709", "295587058")
    //    val targetSZT = udf { (value: String) =>
    //      val cardId = value.split(",")(1)
    //      list.contains(cardId)
    //    }
//            val data = spark.read.textFile("D:/testData/公交处/data/baomaOD/2017-all-sort").map(s=>(s.split(",")(0),1)).rdd.reduceByKey(_+_)
//      .sortBy(_._2,ascending = false).toDF().show(200)
      //.rdd.sortBy(s=>s.split(",")(0))
              //.repartition(1).saveAsTextFile("D:/testData/公交处/data/baomaOD/2017-all-sort")
    //    val d = "14.25\n19.78\n19.45\n14.3\n18.7\n19.5\n19.72\n19.03\n19.37\n18.58\n19.4\n20.93\n13.17\n19.38\n8.85\n18.15\n18.73\n14.78\n19.97\n18.05\n17.85\n19.22\n17.75\n17.87\n7.43\n11.73\n8.22\n7.32\n18.5\n11.62\n7.22\n8.02\n12.07\n17.37\n14.7\n12\n19\n11.78\n17.08\n17.33\n8.17\n7.3\n18.17\n22.32\n16.52\n20.75\n19.68\n7.55\n8.6\n16.5\n7.37\n7.3\n7.27\n7.28\n7.42\n7.43\n8.67\n7.35\n7.25\n16.83\n13.78\n11.55\n7.98\n13.62\n8.13\n11.35\n13.05\n19.85\n18.37\n20.1\n18.2\n11.88\n13.97\n17.58\n11.75\n21.25".split("\n")
    //    spark.sparkContext.parallelize(d).map(s => {
    ////      val split = s.split(",")
    ////      val format = new DecimalFormat("#.0")
    ////      val hour = split(1).toDouble + split(2).toDouble / 60
    //      (s.split("\\.")(0), 1)
    //    }).reduceByKey(_ + _).toDF().show(200)
    val t = "20\n21".split("\n").map(s => s.split("\\.")(0)).map(_.toDouble)
    val array = Array(18.0, 18.0, 18.0, 18.0, 18.0, 14.0, 19.0, 19.0, 17.0, 17.0, 17.0)
    val (mean, variance) = timeMean(t)
    val format = new DecimalFormat("#.00")
    println(format.format(mean), math.sqrt(variance))
    val tt = "17.5\t3.5\n10\t1.12\n21.5\t0.87\n13.5\t0.5\n21\t0.000000001\n20.5\t0.5".split("\n")
    val pro = "0.117647059\n0.235294118\n0.058823529\n0.176470588\n0.205882353\n0.058823529\n0.147058824".split("\n").map(_.toDouble)
//    for (i <- 0 until 240) {
//      var total = 0.0
//      val a = new ArrayBuffer[Double]()
      for (j <- tt.indices) {
        val Array(avg, sd) = tt(j).split("\t").map(_.toDouble)
        val nd = new NormalDistribution(avg, sd)
        //val p = nd.cumulativeProbability(i.toDouble / 10)
        println(nd.cumulativeProbability(10.1))
//        total += pro(j) * p
//        a += pro(j) * p
      }
//      var count = 0
//      a.map(_ / total).foreach(d => {
//        if (d > 0.8){
//          println(i.toDouble / 10,count,d)
//        }
//        count += 1
//      })
//    }

    //    val data0 = "113.987136,22.598301;113.908226,22.482787;"
    //    val data1 = "113.908021,22.482143;113.908426,22.482533;\n113.908259,22.482254;113.936268,22.510643;\n113.936191,22.510769;113.936265,22.509623;\n113.93628,22.509694;113.935645,22.501348;\n113.935576,22.501267;113.908612,22.481908;\n113.908477,22.483114;113.915782,22.486123;\n113.915784,22.486102;113.918085,22.50289;\n113.918085,22.502831;113.908418,22.483211;\n113.908956,22.482767;113.986647,22.598962;\n113.9866,22.598949;113.92791,22.522856;\n113.928116,22.522467;113.986965,22.597525;\n113.908365,22.483066;113.986308,22.598471;\n113.986284,22.598524;113.909031,22.481734;\n113.908757,22.48358;113.987143,22.598421;\n113.986866,22.598756;113.908378,22.48252;\n113.908285,22.482853;113.984711,22.594991;\n113.984577,22.59503;113.986363,22.598811;\n113.98666,22.598287;113.916069,22.500199;\n113.915925,22.500199;113.918285,22.503237;\n113.91824,22.503199;113.90811,22.482546;\n113.908394,22.482901;113.986366,22.598771;\n113.986405,22.59866;113.911642,22.488366;\n113.911667,22.48814;113.907943,22.48201;\n113.908624,22.483146;113.919509,22.508785;\n113.918399,22.511045;113.918808,22.504975;\n113.918945,22.505069;113.908273,22.483175;\n113.906366,22.484611;113.98479,22.595108;\n113.984689,22.594964;113.986528,22.59838;\n113.987136,22.598301;113.908226,22.482787;\n113.909539,22.48304;113.98713,22.598338;\n113.98716,22.598332;113.909307,22.481796;\n113.908439,22.482539;113.926006,22.485103;\n113.908526,22.48304;113.987351,22.59817;\n113.985746,22.59885;113.935729,22.500093;\n113.935855,22.500005;113.907973,22.482055;".split("\n")
    //
    //    val d = data1.map(s => s.replaceAll(";", ","))
    //    for (i <- d.indices) {
    //      //print(distance(data0, data1(i)))
    //      if (distance(data0.replaceAll(";", ",").split(",").map(_.toDouble), d(i).split(",").map(_.toDouble)) < 0.01)
    //        println(data1(i))
    //    }
  }
}

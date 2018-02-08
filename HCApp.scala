package cn.sibat.bus.bmw

import java.io.File
import java.nio.charset.Charset
import java.text.DecimalFormat

import cn.sibat.bus.utils.DateUtil
import cn.sibat.bus.{BMWAPP, BmwOD1}
import com.vividsolutions.jts.geom.{Coordinate, MultiPolygon}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.geotools.data.FeatureSource
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.feature.{FeatureCollection, FeatureIterator}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 预测OD实体
  *
  * @param termId      设备ID
  * @param OD          cluster(od) 聚类后的OD表示
  * @param count       count 频数
  * @param mean        平均值
  * @param variance    方差
  * @param probability 发生该OD的概率
  */
case class PredictOD(termId: String, OD: String, count: Int, mean: Double, variance: Double, probability: Double)

/**
  * 保存聚类后的中心点与对应的类别
  *
  * @param termId           设备ID
  * @param clusterCenterLon 中心点经度
  * @param clusterCenterLat 中心点纬度
  * @param label            类别
  */
case class ClusterModel(termId: String, clusterCenterLon: Double, clusterCenterLat: Double, label: Double)

case class ProbabilityOD(termId: String, O: String, D: String, count: Int, probability: Double, time: String)

/**
  * Created by kong on 2017/7/28.
  */
object HCApp {

  //深圳交通小区信息
  private var POLYGON: Array[(MultiPolygon, String)] = null

  /**
    * 过去经纬度所在的交通小区的zoneId
    * 超出深圳为null
    *
    * @param lon 经度
    * @param lat 纬度
    * @return zoneId
    */
  def zoneId(lon: Double, lat: Double): String = {
    var result = "null"
    val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
    val coord = new Coordinate(lon, lat)
    val point = geometryFactory.createPoint(coord)
    val targetPolygon = POLYGON.filter(t => t._1.contains(point))
    if (!targetPolygon.isEmpty) {
      result = targetPolygon(0)._2
    }
    result
  }

  /**
    * 加载交通小区信息
    *
    * @param path 文件路径
    */
  def read(path: String): Unit = {
    val file = new File(path)
    var shpDataStore: ShapefileDataStore = null
    try {
      shpDataStore = new ShapefileDataStore(file.toURI.toURL)
      shpDataStore.setCharset(Charset.forName("GBK"))
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    val typeName = shpDataStore.getTypeNames()(0)
    var featureSource: FeatureSource[SimpleFeatureType, SimpleFeature] = null
    try {
      featureSource = shpDataStore.getFeatureSource(typeName)
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    var result: FeatureCollection[SimpleFeatureType, SimpleFeature] = null
    try {
      result = featureSource.getFeatures
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    val iterator: FeatureIterator[SimpleFeature] = result.features
    val resultPolygon = new ArrayBuffer[MultiPolygon]()
    val id = new ArrayBuffer[String]()
    try {
      while (iterator.hasNext) {
        val sf = iterator.next()
        id += sf.getID
        val multiPolygon = sf.getDefaultGeometry.asInstanceOf[MultiPolygon]
        resultPolygon += multiPolygon
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      iterator.close()
      shpDataStore.dispose()
    }
    POLYGON = resultPolygon.toArray.zip(id.toIterable)
  }

  /**
    * 把OD数据的起点和终点转化成交通小区编号
    *
    * @param metadata OD数据
    */
  def withZoneId(metadata: Dataset[String]): Unit = {
    read("D:/testData/公交处/交通小区/Zone.shp")
    import metadata.sparkSession.implicits._
    metadata.map(s => {
      val split = s.split(",")
      val date = DateUtil.isWorkDay(split(3))
      val o_lon = split(5).toDouble
      val o_lat = split(6).toDouble
      val d_lon = split(7).toDouble
      val d_lat = split(8).toDouble
      val o_type = zoneId(o_lon, o_lat)
      val d_type = zoneId(d_lon, d_lat)
      s.replace("[", "").replace("]", "") + "," + o_type + "," + d_type + "," + date
    }).rdd.sortBy(s => s.split(",")(0) + "," + s.split(",")(1)).repartition(1).saveAsTextFile("D:/testData/公交处/data/baomaOD/2017-06-1-sort-type")
  }

  /**
    * 把OD数据的起点和终点转化成交通小区编号
    *
    * @param metadata OD数据
    */
  def withZoneId(metadata: DataFrame, savePath: String): Unit = {
    read("D:/testData/公交处/交通小区/Zone.shp")
    import metadata.sparkSession.implicits._
    metadata.map(s => {
      val departTime = s.getString(s.fieldIndex("departTime"))
      val date = DateUtil.isWorkDay(departTime)
      val o_lon = s.getDouble(s.fieldIndex("departLon"))
      val o_lat = s.getDouble(s.fieldIndex("departLat"))
      val d_lon = s.getDouble(s.fieldIndex("arrivalLon"))
      val d_lat = s.getDouble(s.fieldIndex("arrivalLat"))
      val o_type = zoneId(o_lon, o_lat)
      val d_type = zoneId(d_lon, d_lat)
      s.mkString(",") + "," + o_type + "," + d_type + "," + date
    }).show(false) //.rdd.sortBy(s => s.split(",")(0)).repartition(1).saveAsTextFile(savePath)
  }

  /**
    * 概率OD
    *
    * @param metadata ds
    */
  def probabilityOD(metadata: Dataset[String]): Unit = {
    import metadata.sparkSession.implicits._
    val format = new DecimalFormat("#.00")
    metadata.map { s =>
      val split = s.split(",")
      val Array(hour, minute, second) = split(3).split(" ")(1).split(":")
      val t = format.format(hour.toDouble + minute.toDouble / 60.0)
      s + "," + t
    }.groupByKey(s => s.split(",")(0) + "," + s.split(",")(14)).flatMapGroups((s, it) => {
      val arr = it.toArray
      val length = arr.length
      arr.groupBy(line => line.split(",")(14) + "," + line.split(",")(15)).map { group =>
        val set = group._2.map(str => str.split(",")(str.split(",").length - 1).toDouble).toSet
        val Array(o, d) = group._1.split(",")
        ProbabilityOD(s.split(",")(0), o, d, group._2.length, group._2.length.toDouble / length, set.mkString("-"))
      }
    }).show(false)
  }

  /**
    * 以时间t为主，聚类od的概率模型
    *
    * @param metadata ds
    */
  def normalProbability(metadata: Dataset[String]): Unit = {
    import metadata.sparkSession.implicits._
    val model = metadata.groupByKey(s => s.split(",")(0))
      .mapGroups((s, it) => {
        val arr = it.toArray
        val length = arr.length
        val data = arr.flatMap { s =>
          val result = new ArrayBuffer[Array[Double]]()
          result += s.split(",").slice(5, 7).map(_.toDouble)
          result += s.split(",").slice(7, 9).map(_.toDouble)
          result
        }.zipWithIndex.map(tp => (tp._2.toString, tp._1))
        val map = HierarchicalClustering.run(data, 0.005)

        val clustering = arr.map { s =>
          val split = s.split(",")
          val o_key = map.getOrElse(split.slice(5, 7).mkString(",") + ";", 100000.0)
          val d_key = map.getOrElse(split.slice(7, 9).mkString(",") + ";", 100000.0)
          s + "," + o_key + "," + d_key
        }
        val result = clustering.groupBy { cluster =>
          val split = cluster.split(",")
          split(split.length - 2) + "," + split(split.length - 1)
        }.map { group =>
          val count = group._2.length
          val probability = count / length.toDouble
          val (mean, variance) = BMWAPP.timeMean(group._2.map { line =>
            val Array(hour, minute, second) = line.split(",")(3).split(" ")(1).split(":")
            hour.toDouble + minute.toDouble / 60
          })
          PredictOD(s, group._1, count, mean, variance, probability)
        }
        (s, map, result.toArray)
      })

    //聚类中心
    model.flatMap(row => row._2.groupBy(tp => tp._2).map(t => {
      var sum = (0.0, 0.0)
      t._2.foreach { map =>
        val Array(lon, lat) = map._1.replace(";", "").split(",").map(_.toDouble)
        sum = sum.copy(sum._1 + lon, sum._2 + lat)
      }
      ClusterModel(row._1, sum._1 / t._2.size, sum._2 / t._2.size, t._1)
    })).write.parquet("D:/testData/公交处/data/baomaOD/clusterModel")
    model.flatMap(_._3).write.parquet("D:/testData/公交处/data/baomaOD/PredictOD")
  }

  /**
    * 以时间t为主，聚类od的概率模型
    *
    * @param metadata ds
    */
  def clusterModel(metadata: Dataset[String]): Unit = {
    import metadata.sparkSession.implicits._
    val model = metadata.groupByKey(s => s.split(",")(0))
      .flatMapGroups((s, it) => {
        val arr = it.toArray
        val data = arr.flatMap { s =>
          val result = new ArrayBuffer[Array[Double]]()
          result += s.split(",").slice(5, 7).map(_.toDouble)
          result += s.split(",").slice(7, 9).map(_.toDouble)
          result
        }.zipWithIndex.map(tp => (tp._2.toString, tp._1))
        val map = HierarchicalClustering.run(data, 0.005)
        val result = map.groupBy(tp => tp._2).map(t => {
          var sum = (0.0, 0.0)
          t._2.foreach { map =>
            val Array(lon, lat) = map._1.replace(";", "").split(",").map(_.toDouble)
            sum = sum.copy(sum._1 + lon, sum._2 + lat)
          }
          Array(sum._1 / t._2.size, sum._2 / t._2.size)
        })
        result
      }).rdd.map(arr => (Random.nextInt(100).toString, arr)).groupBy(t => t._1).flatMap(tuple => {
      val map = HierarchicalClustering.run(tuple._2.toArray, 0.005)
      val result = map.groupBy(tp => tp._2).map(t => {
        var sum = (0.0, 0.0)
        t._2.foreach { map =>
          val Array(lon, lat) = map._1.replace(";", "").split(",").map(_.toDouble)
          sum = sum.copy(sum._1 + lon, sum._2 + lat)
        }
        Array(sum._1 / t._2.size, sum._2 / t._2.size)
      })
      result
    }).zipWithIndex().map(t => (t._2.toString, t._1)).collect()

    metadata.sparkSession.sparkContext.parallelize(HierarchicalClustering.run(model, 0.005).toSeq)
      .toDF("clusterCenter","type").write.parquet("D:/testData/公交处/data/baomaOD/clusterCenter")
  }

  def main(args: Array[String]) {

    /**
      * 对每一个用户进行建模，首先进行聚类
      * 根据聚类的结果构造聚类OD
      * 计算时间概率分布的平均值与方差
      * 计算分布概率
      */
    val spark = SparkSession.builder().appName("HCApp").master("local[*]").getOrCreate()
    import spark.implicits._
    //tuple3(userId,clusterModel,)
    val metadata = spark.read.textFile("D:/testData/公交处/data/baomaOD/2017-06-1-sort")
    withZoneId(metadata)

    //    val testSet = spark.read.textFile("D:/testData/公交处/data/baomaOD/2017-all-sort-type").map(s => {
    //      val split = s.split(",")
    //      BmwOD1(split(0), split(2), split(3), split(4), split(5).toDouble, split(6).toDouble, split(7).toDouble, split(8).toDouble,
    //        split(9).toInt, split(10).toDouble, split(11).toDouble, split(12).toDouble, split(13),split(14),split(15))
    //    }).toDF()
    //    val clusterModel = new BMWClusterModel(spark)
    //    clusterModel.loadModel("D:/testData/公交处/data/baomaOD/clusterModel", "D:/testData/公交处/data/baomaOD/probabilityOD")
    //    val result = clusterModel.transform1(testSet)
    //    result.repartition(1).rdd.saveAsTextFile("D:/testData/公交处/data/baomaOD/transform")
    //    val qu = udf { (predict: String, o_type: Double, d_type: Double) =>
    //      predict.contains(o_type + "," + d_type)
    //    }
    //    val correct = result.filter(qu(col("predict"), col("o_type"), col("d_type"))).count()
    //    println(correct.toDouble / result.count())
  }
}

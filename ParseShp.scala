package cn.sibat.metro.truckOD

import java.io.File
import java.nio.charset.Charset

import com.vividsolutions.jts.geom.{Coordinate, MultiPolygon}
import org.apache.spark.sql.{Dataset, SparkSession}

import org.geotools.data.FeatureSource
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.feature.{FeatureCollection, FeatureIterator}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer

/**
  * OD匹配，找出区域内的起始点或者出发点，匹配OD到固定范围
  * Created by wing1995 on 2017/9/11.
  */
object ParseShp {

    //货车数据分析范围
    private var POLYGON: Array[(MultiPolygon, String)] = _

    /**
      * 加载货车区域shp文件
      *
      * @param path shp文件路径
      */
    def read(path: String, shpArea: String): Unit = {
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
        val zoneName = new ArrayBuffer[String]()
        try {
            //将所有的polygon都放入数组中
            while (iterator.hasNext) {
                val sf = iterator.next()
                val attributeName = shpArea match {
                    case "WYID" => sf.getAttribute(shpArea).toString
                    case "JDNAME" => sf.getAttribute(shpArea).toString
                    case "QUNAME" => sf.getAttribute(shpArea).toString
                }
                zoneName += attributeName
                val multiPolygon = sf.getDefaultGeometry.asInstanceOf[MultiPolygon]
                resultPolygon += multiPolygon
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            iterator.close()
            shpDataStore.dispose()
        }
        POLYGON = resultPolygon.toArray.zip(zoneName)
    }

    /**
      * 获取经纬度所在的区域名称
      * 若都不在该shp文件的范围内则为null
      * @param lon 经度
      * @param lat 纬度
      * @return zoneId 当前经纬点所在的区域ID
      */
    def getZoneName(lon: Double, lat: Double): String = {
        var result = "null"
        val geometryFactory = JTSFactoryFinder.getGeometryFactory()
        val coord = new Coordinate(lon, lat)
        val point = geometryFactory.createPoint(coord)
        val targetPolygon = POLYGON.filter(t => t._1.contains(point)) //过滤区域外的点
        if (!targetPolygon.isEmpty) {
            result = targetPolygon(0)._2 //若该点属于货车分析区域，则将结果存储否则为null
        }
        result
    }

    /**
      * 把货车OD数据的起点和终点添加对应的区域名称或ID
      *
      * @param metadata OD数据
      */
    def withZoneName(metadata: Dataset[String], shpFile: String, shpArea: String, savePath: String): Unit = {
        read(shpFile, shpArea)
        import metadata.sparkSession.implicits._
        metadata.map(s => {
            val split = s.split(",")
            val o_lon = split(1).toDouble
            val o_lat = split(2).toDouble
            val d_lon = split(3).toDouble
            val d_lat = split(4).toDouble
            val o_type = getZoneName(o_lon, o_lat)
            val d_type = getZoneName(d_lon, d_lat)
            s + "," + o_type + "," + d_type
        }).rdd.sortBy(s => s.split(",")(1) + "," + s.split(",")(2)).repartition(1).saveAsTextFile(savePath)
    }

    def main(args: Array[String]) {

//        val spark = SparkSession.builder()
//          .appName("TruckApp")
//          .master("local[*]")
//          .config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my")
//          .getOrCreate()
//        val metadata = spark.read.textFile("E:\\trafficDataAnalysis\\testData\\truckData\\part-r-00000")
//        val savePath = "E:\\货车OD\\tables"
//        withZoneId(metadata, savePath)
        val shpPath = args(0)
        val shpArea = shpPath match {
            case "行政区2017.shp" => "QUNAME"
            case "街道2017.shp" => "JDNAME"
            case "交通小区.shp" => "WYID"
        }
        read(shpPath, shpArea)
        val result = getZoneName(113.868,22.711)
        println(result)
    }
}

case class TruckOD(cardId: String, departLon: Double, departLat: Double, arrivalLon: Double, arrivalLat: Double)

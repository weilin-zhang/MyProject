package cn.sibat.truck

import java.io.File
import java.nio.charset.Charset

import org.apache.spark.sql.functions._
import com.vividsolutions.jts.geom._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.geotools.data.FeatureSource
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.feature.{FeatureCollection, FeatureIterator}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.matching.Regex
import cn.sibat.bus.utils.LocationUtil

/**
  * 本地OD匹配，找出区域内的起始点或者出发点，匹配OD到固定范围
  * Created by wing1995 on 2017/9/11.
  */
class ParseShp(shpPath: String) extends Serializable{

    //区域分析范围：Array(区域，区域ID或Name)
    var POLYGON:  Array[(MultiPolygon, String)] = _

    //路网的线路范围:Array(道路区域，该道路地理信息区域对应的道路属性)
    var ROADLINE: Array[(LineString, Road)] = _
    //主要存储深南大道、滨河大道以及北环大道道路缓冲信息
    var GEOMETRY: Array[(Geometry, String)] = _

    /**
      * 加载货车区域shp文件并将区域信息存储到数组POLYGON
      */
    def readShp(): ParseShp = {

        val file = new File(shpPath)
        var shpDataStore: ShapefileDataStore = null
        val pattern = new Regex("[^/]+(?=.shp)") //匹配文件名（去除后缀）
        val shpName = pattern.findAllIn(shpPath).mkString(",")
        try {
            shpDataStore = new ShapefileDataStore(file.toURL)
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
                val attributeName = shpName match {
                    case "jtxq" => "WYID"
                    case "jd" => "JDNAME"
                    case "xzq" => "QUNAME"
                }
                zoneName += sf.getAttribute(attributeName).toString
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
        this
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
        val targetPolygon = this.POLYGON.filter(t => t._1.contains(point)) //过滤区域外的点
        if (targetPolygon.nonEmpty) {
            result = targetPolygon(0)._2 //若该点属于货车分析区域，则将结果存储否则为null
        }
        result
    }

    /**
      * udf函数
      */
    val getZoneNameUdf: UserDefinedFunction = udf((lon: Double, lat: Double) => {
        var result = "null"
        val geometryFactory = JTSFactoryFinder.getGeometryFactory()
        val coord = new Coordinate(lon, lat)
        val point = geometryFactory.createPoint(coord)
        val targetPolygon = POLYGON.filter(t => t._1.contains(point)) //过滤区域外的点
        if (targetPolygon.nonEmpty) {
            result = targetPolygon(0)._2 //若该点属于货车分析区域，则将结果存储否则为null
        }
        result
    })

    /**
      * 读取广东省路网文件
      * @param distance 缓冲距离（单位：度）其中，1km=0.009度
      * @return
      */
    def readPolylineShp(distance: Double): ParseShp = {
        val roadLineBuffer = new ArrayBuffer[(LineString, Road)]()
        val geometryBuffer = new ArrayBuffer[(Geometry, String)]()

        val file = new File(shpPath)
        var shpDataStore: ShapefileDataStore = null
        try {
            shpDataStore = new ShapefileDataStore(file.toURL)
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
        try {
            while (iterator.hasNext) {
                // Data Reader
                val feature = iterator.next
                val geom = feature.getDefaultGeometry.asInstanceOf[MultiLineString]

                val roadID = feature.getAttribute("ID").toString //道路ID
                val roadName = feature.getAttribute("Name").toString //道路名称
                val routeLevel = feature.getAttribute("RouteLevel").toString.toInt //道路等级
                val width = feature.getAttribute("Width").toString.toDouble //道路宽度
                val length = feature.getAttribute("Length").toString.toDouble //道路长度

                val road = Road(roadID, roadName, routeLevel, width, length)
                val geometry = geom.getGeometryN(0).asInstanceOf[LineString] //道路地理信息

                roadLineBuffer += ((geometry, road))

                val geoBuffer  = geom.buffer(distance)
                if(roadName=="深南大道" || roadName=="滨河大道" || roadName=="北环大道") {
                    geometryBuffer += ((geoBuffer, roadName))
                }
            }
        }
        catch {
            case e: Exception => {
                e.printStackTrace()
            }
        } finally {
            iterator.close()
            shpDataStore.dispose()
        }
        ROADLINE = roadLineBuffer.toArray
        GEOMETRY = geometryBuffer.toArray
        this
    }

    /**
      * 获取经纬度对应的道路名称
      * @param lon 纬度
      * @param lat 经度
      * @return roadName
      */
    def getRoadName(lon: Double, lat: Double): String = {
        var roadName = "null"
        val geometryFactory = JTSFactoryFinder.getGeometryFactory()
        val coord = new Coordinate(lon, lat)
        val point = geometryFactory.createPoint(coord)
        val targetPolygon = this.ROADLINE.filter(t => t._1.contains(point)) //过滤区域外的点
        if (targetPolygon.nonEmpty) {
            roadName = targetPolygon(0)._2.roadName //若该点属于深南大道或者滨河大道或者北环大道，则将结果返回否则为null
        }
        roadName
    }

    /**
      * 获取经纬度对应的道路名称
      * @param lon 纬度
      * @param lat 经度
      * @return roadName
      */
    def getThreeRoadName(lon: Double, lat: Double): String = {
        var roadName = "null"
        val geometryFactory = JTSFactoryFinder.getGeometryFactory()
        val coord = new Coordinate(lon, lat)
        val point = geometryFactory.createPoint(coord)
        val target = this.GEOMETRY.filter(t => t._1.contains(point))
        if (target.nonEmpty) roadName = target(0)._2
        roadName
    }

    /**
      * 获取公交站点GPS文件
      * @param metroStationFile 站点文件路径
      * @return 站点内容存储到array
      */
    def readMetroFile(metroStationFile: String): Array[String] = {
        val gpsBuffer = new ArrayBuffer[String]()
        val lines = Source.fromFile(metroStationFile).getLines()
        lines.foreach(line => {
            gpsBuffer += line
        })
        gpsBuffer.toArray
    }
}

//可以直接通过调用伴生对象生成polygon
object ParseShp{
    def apply(shpPath: String): ParseShp = new ParseShp(shpPath)

    def main(args: Array[String]) {
        var shpPath = "E:/trafficDataAnalysis/Guotu/行政区划2017/行政区2017.shp"//不同等级下划分的区域shp文件路径
        if (args.length > 0) {
            shpPath = args(0)
        }
        val parseShp = ParseShp(shpPath).readShp()
        val Array(lat, lon) = LocationUtil.gcj02_To_84(22.565659966852593,114.04175864754319).split(",") //区域划分的shp文件的坐标系属于wgs
        println(parseShp.getZoneName(lon.toDouble,lat.toDouble))

//        val parseShp = ParseShp(shpPath).readPolylineShp(0.0009) //一公里等于0.009度，这里的distance的单位度，也就是传入给道路构建100米的缓存区
//        val gpsArray = parseShp.readMetroFile("F:\\模块化工作\\trafficDataAnalysis\\subway_station_GPS")
//        gpsArray.foreach(line => {
//            val arr = line.split(",")
////            val lon = arr(6).toDouble
////            val lat = arr(5).toDouble
//            val result = parseShp.getThreeRoadName(arr(5).toDouble, arr(4).toDouble)
//            if (!result.equals("null")) println(line + "," + result)
//        })
    }
}

/**
  * 路网信息提取的数据
  * @param roadId 道路ID
  * @param roadName  道路名称
  * @param roadLevel 道路等级
  * @param roadWidth 道路宽度
  * @param roadLength 道路长度
  */
case class Road(roadId: String, roadName: String, roadLevel: Int, roadWidth: Double, roadLength: Double)
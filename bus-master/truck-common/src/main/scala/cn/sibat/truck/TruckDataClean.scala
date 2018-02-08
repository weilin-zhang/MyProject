package cn.sibat.truck

import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, _}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 货车数据清洗（主要是经纬度以及时间格式处理）
  * Created by wing on 2017/9/26.
  */
class TruckDataClean extends Serializable{
    /**
      * 数据格式化，由于数据的后面几个关于高度和速度的字段顺序混乱，因此直接选取经纬度数据
      * 文本方式读入数据
      * @return ds
      */
    def formatUtilForText(ds: Dataset[String]): DataFrame = {
        import ds.sparkSession.implicits._
        val filteredDf = ds.map(_.split(",")).filter(_.length == 8)
        filteredDf.map(arr => {
            val carId = arr(0)
            val lon = arr(1).toDouble
            val lat = arr(2).toDouble
            val time = arr(3)
            val date = time.split(" ")(0)
            TruckData(carId, lon, lat, time, date)
        }).toDF()
    }

    /**
      * csv文件方式读取数据
      * 数据格式化，由于数据的后面几个关于高度和速度的字段顺序混乱，因此直接选取经纬度数据
      * @return df
      */
    def formatUtilForCsv(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._
        val filteredDf = df.filter(_.length == 8)
        filteredDf.map(row => {
            val carId = row.getString(0)
            val lon = row.getString(1).toDouble
            val lat = row.getString(2).toDouble
            val time = row.getString(3)
            val date = time.split(" ")(0)
            TruckData(carId, lon, lat, time, date)
        }).toDF()
    }

    /**
      * 计算时间差的工具
      * @param firstTime 上一个时间点
      * @param lastTime 下一个时间点
      * @return result(row)
      */
    def timeDiffUtil(firstTime: String, lastTime: String):  Int = {
            var result = -1L
            try {
                val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                result = (sdf.parse(lastTime).getTime - sdf.parse(firstTime).getTime) / 1000
            }catch {
                case e:Exception => e.printStackTrace()
            }
            result.toInt
    }

    /**
      * 时间解析的工具，为了得到正确格式的时间数据
      * @param upTime GPS 上传时间
      * @return
      */
    def timeParseUtil(upTime: String): String = {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        try {
            sdf.parse(upTime).toString
        } catch {
            case _: Exception => "errorDate"
        }
    }

    /**
      * udf函数
      */
    val timeParseUdf: UserDefinedFunction = udf((upTime: String) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        try {
            sdf.parse(upTime).toString
        } catch {
            case _: Exception => "errorDate"
        }
    })

    /**
      * 计算两个经纬度点之间的距离(m)
      * @param lon1 第一个点的经度
      * @param lat1 第一个点的纬度
      * @param lon2 第二个点的经度
      * @param lat2 第二个点的纬度
      * @return
      */
    def distanceUtil(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
        val EARTH_RADIUS: Double = 6378137
        val radLat1: Double = lat1 * math.Pi / 180
        val radLat2: Double = lat2 * math.Pi / 180
        val a: Double = radLat1 - radLat2
        val b: Double = lon1 * math.Pi / 180 - lon2 * math.Pi / 180
        val row: Double = 2 * math.asin(Math.sqrt(math.pow(Math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
        row * EARTH_RADIUS
    }

    /**
      * 利用前一个点和后一个点的位置和时间求位移和平均速度
      * 1、过滤漂移点（平均速度过大）以及没有移动的点（部分车在一个地方不动但是仍然没有熄火）
      * 2、时间解析错误的数据
      * @return
      */
    def filterUtil(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._
        df.withColumn("timeStamp", timeParseUdf(col("time")))
        .filter(col("timeStamp")=!="errorDate")
        .drop(col("timeStamp"))
        .groupByKey(row => row.getString(row.fieldIndex("carId")) + row.getString(row.fieldIndex("time")).split(" ")(0)).flatMapGroups((key, iter) => {
            val result = new ArrayBuffer[String]
            var firstTime = "null"
            var firstLon = 0.0
            var firstLat = 0.0
            iter.toArray.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
                //初始化第一条数据
                if (result.isEmpty) {
                    firstTime = row.getString(row.fieldIndex("time"))
                    firstLon = row.getDouble(row.fieldIndex("lon"))
                    firstLat = row.getDouble(row.fieldIndex("lat"))
                    result.+=(row.mkString(",") + ",0,0.0,0.0")
                } else {
                    //后面的点以平移的方式对前面的点求时间差和平移速度
                    val lastTime = row.getString(row.fieldIndex("time"))
                    val lastLon = row.getDouble(row.fieldIndex("lon"))
                    val lastLat = row.getDouble(row.fieldIndex("lat"))
                    val elapsedTime = timeDiffUtil(firstTime, lastTime)
                    val movement = distanceUtil(firstLon, firstLat, lastLon, lastLat)
                    var avgSpeed = 0.0
                    //过滤掉不动的点
                    if (!elapsedTime.equals(0) && !movement.equals(0.0)) {
                        avgSpeed = movement / elapsedTime //平均速度
                        result.+=(row.mkString(",") + "," + elapsedTime + "," + movement + "," + avgSpeed )
                    }
                    firstTime = lastTime
                    firstLon = lastLon
                    firstLat = lastLat
                }
            })
            result
        }).map(record => {
        val arr = record.split(",")
        (arr(0), arr(1).toDouble, arr(2).toDouble, arr(3), arr(4), arr(5).toInt, arr(6).toDouble, arr(7).toDouble)
    }).toDF("carId", "lon", "lat", "time", "date", "elapsedTime", "movement", "avgSpeed")
        .filter(col("avgSpeed") < 33.33 && col("avgSpeed") > 0)
    }
}

object TruckDataClean{
    //语法糖
    def apply(): TruckDataClean = new TruckDataClean()

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Truck Data Processing")
            .setMaster("local[*]")
            .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[TruckData]))

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._
        val dataPath = "truckData/20160104.csv"
//        val df = spark.read.format("csv").csv(dataPath)
        val ds = spark.read.textFile(dataPath)
        val formatData = TruckDataClean().formatUtilForText(ds)
        val cleanData = TruckDataClean().filterUtil(formatData)

        cleanData.show()

//        val intervalFor120 = cleanData.filter(col("elapsedTime") >= 120*60).groupByKey(row => row.getString(row.fieldIndex("carId"))).count().count()
//        val intervalFor60 = cleanData.filter(col("elapsedTime") >= 60*60).groupByKey(row => row.getString(row.fieldIndex("carId"))).count().count()
//        val intervalFor30 = cleanData.filter(col("elapsedTime") >= 30*60).groupByKey(row => row.getString(row.fieldIndex("carId"))).count().count()
//        println(cleanData.groupBy(col("carId")).count().count(), intervalFor30, intervalFor60, intervalFor120)//13942,7673,6455,4006,3873,3686,3615
//        cleanData.filter(col("carId")==="粤BZ9988").groupByKey(row => Math.floor(row.getInt(row.fieldIndex("elapsedTime"))/60)).count().show()

        spark.stop()
    }
}

/**
  * 货车数据
  * @param carId 车牌号
  * @param lon 经度
  * @param lat 纬度
  * @param time 定位时间
  */
case class TruckData(carId: String, lon: Double, lat: Double, time: String, date: String)

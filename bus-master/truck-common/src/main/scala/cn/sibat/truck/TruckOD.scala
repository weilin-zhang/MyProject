package cn.sibat.truck

import org.apache.spark.{SparkConf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 计算货车OD，包括OD时长，OD距离，OD区域
  * Created by wing on 2017/10/8.
  */
class TruckOD extends Serializable{
    /**
      * 获取货车每日OD数据，OD分两种情况考虑
      * OD根据熄火时间分割。
      * 首先，根据第一条GPS记录初始化一个OD记录，若第二个点到第一个点的时间间隔少于30分钟则将第二个点合并到第一个点中求得OD距离，以此类推并获取货车出行距离和出行时长
      * 然后，当熄火时间超过60分钟切当前数据不是最后一条数据时，将最近的一次OD元素取出，将OD记录中的第一条GPS记录的经纬度数据与最后一个GPS记录作对比，求OD时间差，从而更新最后一条OD中的数据，并根据新的OD中的第一个元素新建一个OD记录
      * 最后，
      * @param cleanData 清洗后的数据
      * @return
      */
    def getOd(cleanData: DataFrame): DataFrame = {

        import cleanData.sparkSession.implicits._
        val groupedData = cleanData.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," +  row.getString(row.fieldIndex("date")))

        val odData = groupedData.flatMapGroups((key, iter) => {

            val truckOdBuffer = new ArrayBuffer[TruckOdData]()
            val carId = key.split(",")(0)
            val date = key.split(",")(1)
            val records = iter.toArray.sortBy(row => row.getString(row.fieldIndex("time")))

            var firstTime = "null"
            var firstLon = 0.0
            var firstLat = 0.0
            var dis = 0.0
            var count = 1
            val length = records.length
            records.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
                if (truckOdBuffer.isEmpty && count < length) {
                    //初始化第一个OD记录
                    firstLon = row.getDouble(row.fieldIndex("lon"))
                    firstLat = row.getDouble(row.fieldIndex("lat"))
                    firstTime = row.getString(row.fieldIndex("time"))
                    //起点数据：车牌号，出发经度，出发纬度，到达经度，到达纬度，出发时间，到达时间，花费时间，各点总距离，日期
                    truckOdBuffer.+=(TruckOdData(carId, oLon = firstLon, oLat = firstLat, dLon = 0.0, dLat = 0.0, oTime = firstTime, dTime = "null", odTime = 0, odDistance = 0.0, date = date))
                } else if(truckOdBuffer.nonEmpty) {
                    val lastTime = row.getString(row.fieldIndex("time"))
                    val lastLon = row.getDouble(row.fieldIndex("lon"))
                    val lastLat = row.getDouble(row.fieldIndex("lat"))
                    val elapsedTime = row.getInt(row.fieldIndex("elapsedTime"))
                    val movement = row.getDouble(row.fieldIndex("movement"))
                    //od切分
                    if (elapsedTime > 60*60 && count < length) {
                        val firstOD = truckOdBuffer(truckOdBuffer.length - 1)
                        val odTime = TruckDataClean().timeDiffUtil(firstOD.oTime, lastTime)
                        val odDistance = dis
                        val nowOD = firstOD.copy(carId, firstOD.oLon, firstOD.oLat, firstLon, firstLat, firstOD.oTime, firstTime, odTime, odDistance, date)
                        truckOdBuffer.remove(truckOdBuffer.length - 1)
                        truckOdBuffer.+=(nowOD) //替代原来初始化的OD
                        truckOdBuffer.+=(TruckOdData(carId, oLon = lastLon, oLat = lastLat, dLon = 0.0, dLat = 0.0, oTime = lastTime, dTime = "null", odTime = 0, odDistance = 0.0, date)) //并为当前点新建一个OD
                    } else if(count == length) { //最后一个行程
                        val lastOD = truckOdBuffer(truckOdBuffer.length - 1)
                        val odTime = TruckDataClean.apply().timeDiffUtil(lastOD.oTime, lastTime)
                        val odDistance = dis + movement
                        val nowOD = lastOD.copy(carId, lastOD.oLon, lastOD.oLat, lastLon, lastLat, lastOD.oTime, lastTime, odTime, odDistance, date)
                        truckOdBuffer.remove(truckOdBuffer.length - 1)
                        if (!odDistance.equals(0.0)) {
                            truckOdBuffer.+=(nowOD)
                        }
                    } else {
                        dis += movement
                    }
                    firstTime = lastTime
                    firstLon = lastLon
                    firstLat = lastLat
                }
                count += 1
            })
            truckOdBuffer
            })
        odData.sort("carId", "oTime").toDF()
    }
}

object TruckOD{
    //语法糖
    def apply(): TruckOD= new TruckOD()

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Truck Data Processing")
            .setMaster("local[*]")
            .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")

        val spark = SparkSession.builder().config(conf).getOrCreate()
        var dataPath = "truckData/12yue/20161205.csv"
        if (args.length > 0) {
            dataPath = args(0)
        }
        val ds = spark.read.textFile(dataPath)
        val formatData = TruckDataClean.apply().formatUtilForText(ds).toDF()
        val cleanData = TruckDataClean.apply().filterUtil(formatData)

        val odData = TruckOD.apply().getOd(cleanData)
        odData.show()

        spark.stop()
    }
}

/**
  * 货车OD数据
  * @param carId 卡号
  * @param oLon 起点经度
  * @param oLat 起点纬度
  * @param dLon 终点经度
  * @param dLat 终点纬度
  * @param oTime 出发时间
  * @param dTime 到达时间
  * @param odTime 出行时长
  * @param odDistance 出行距离，是各个点的累加距离
  */
case class TruckOdData(carId: String, oLon: Double, oLat: Double, dLon: Double, dLat: Double, oTime: String, dTime: String, odTime: Int, odDistance: Double, date: String)
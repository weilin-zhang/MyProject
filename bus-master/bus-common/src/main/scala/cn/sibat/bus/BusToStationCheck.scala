package cn.sibat.bus

import java.text.SimpleDateFormat

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 这里训练数据选取的是2017年10月9号到10月15号这周的数据，而测试数据选取的是9月14号的数据
  * 公交车GPS数据接收时间和实际上传时间的基本数据统计描述
  * Created by wing on 2017/9/20.
  */
class BusToStationCheck extends Serializable{
    /**
      * 计算GPS数据延迟时间差（s）
      * @param sysTime 系统接收时间
      * @param upTime 数据上传时间
      * @return result
      */
    def dealTime(sysTime: String, upTime: String): Double = {
         try {
              val sdf = new SimpleDateFormat("yy-M-d H:m:s")
              (sdf.parse(sysTime).getTime - sdf.parse(upTime).getTime) / 1000
         }catch {
             case _ :Exception =>  Int.MaxValue
         }
    }

    /**
      * 数据格式化，并将时间格式错误的数据过滤
      * @param sqlContext SQLContext
      * @param rdd RDD
      * @return df
      */
    def dataFormat(sqlContext: SQLContext, rdd: RDD[String]): DataFrame = {
        import sqlContext.implicits._
        rdd.map(s => {
            val arr = s.split(",")
            val carId = arr(3)
            val sysTime = arr(0)
            val upTime = arr(11)
            val timeDiff = dealTime(sysTime, upTime)
            val date = sysTime.split(" ")(0)
            (carId, sysTime, upTime, timeDiff, date)
        }).toDF("carId", "sysTime", "upTime", "timeDiff", "date")
            .filter(col("timeDiff") =!= Int.MaxValue)
    }

    /**
      * 得到时间延迟分布，由于电子站牌将数据延迟超过5分钟的直接过滤，导致我们只计算到延迟超过5分钟的数据占比
      * @param df 格式化并清洗以后的公交车GPS数据
      * @return
      */
    def checkDataDelay(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._
        df.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("date")))
            .mapGroups((key, iter) => {
                val carId = key.split(",")(0) //车牌号
                val date = key.split(",")(1) //日期
                var count = 0.0 //每辆车发射的GPS数据量。两个整数相除等于整数
                var errCount = 0.0 //设备终端上传时间出错的数据量
                var delayCount = 0.0 //延迟数据量
                var delayLess60Seconds = 0.0 //延迟1min以内
                var delayFor180Seconds = 0.0 //延迟1min——3min
                var delayFor300Seconds = 0.0 //延迟3min——5min
                var delayMore300Seconds = 0.0 //延迟5min以上
                var allDelaySeconds = 0.0 //总延迟时间
                var avgDelaySeconds = 0.0 //平均延迟时间
                var maxDelaySeconds = 0.0 //最长延迟时间
                var maxDelayTime = "null" //最长延迟时间对应的时间
                iter.foreach(row => {
                    val timeDiff = row.getDouble(row.fieldIndex("timeDiff"))
                    if(timeDiff > 0) {
                        delayCount += 1
                        if (timeDiff <= 60) delayLess60Seconds += 1
                        else if(timeDiff > 60 && timeDiff <= 180) delayFor180Seconds += 1
                        else if(timeDiff > 180 && timeDiff <= 300) delayFor300Seconds += 1
                        else if (timeDiff > 300) delayMore300Seconds += 1
                        allDelaySeconds += timeDiff
                        if (timeDiff > maxDelaySeconds) {
                            maxDelaySeconds = timeDiff
                            maxDelayTime = row.getString(row.fieldIndex("upTime"))
                        }
                    } else if(timeDiff < 0) errCount += 1
                    count += 1
                })
                if (delayCount != 0) avgDelaySeconds = allDelaySeconds / delayCount
                (carId, count, errCount/count, delayCount/count, delayLess60Seconds/count, delayFor180Seconds/count, delayFor300Seconds/count, delayMore300Seconds/count, avgDelaySeconds, maxDelaySeconds, maxDelayTime, date)
            }).toDF("carId", "count", "errPercent", "delayPercent", "delayLess60Percent", "delayFor180Percent", "delayFor300Percent", "delayMore300Percent", "avgDelaySeconds", "maxDelaySeconds", "maxDelayTime", "date")
    }
}

object BusToStationCheck {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()//.setMaster("local[*]").setAppName("CarFreeDayAPP")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)
        val rdd = sc.hadoopFile[LongWritable, Text, TextInputFormat]("busData/*", 1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GB2312")).filter(_.split(",").length > 14)
        import sqlContext.implicits._
        val busToStationCheck = new BusToStationCheck
        val formatData = busToStationCheck.dataFormat(sqlContext, rdd)
        busToStationCheck.checkDataDelay(formatData).map(_.mkString(",")).repartition(1).rdd.saveAsTextFile("busDataNewDelay")
        //busToStationCheck.checkDataDelay(formatData).describe("delayFor180Percent").show()
        sc.stop()
    }
}

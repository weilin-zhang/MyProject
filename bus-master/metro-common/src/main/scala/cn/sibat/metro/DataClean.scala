package cn.sibat.metro

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, _}

/**
  * 深圳通数据清洗工具，使用链式写法，给每一个异常条件（清洗规则）添加方法
  * Created by wing1995 on 2017/5/4.
  */
class DataClean (val data: DataFrame) {
    import data.sparkSession.implicits._

  /**
    * 将清洗完的数据返回
    * @return DataFrame
    */
  def toDF: DataFrame = this.data

  /**
    * 构造伴生对象，返回对象本身，实现链式写法
    * @param df 清洗后的DataFrame
    * @return 原对象 DataClean
    */
  private def newUtils(df: DataFrame): DataClean = new DataClean(df)

//  def errTime(): DataClean = {
//     注意新的数据中有部分设备出错，例如20170731的二号线上的地铁入站打卡日期都是20110701，后面再做判断
//  }

  /**
    * 针对深圳通原始数据，添加日期列
    * 将深圳通数据文件中的打卡时间为每天的4：00到次日4：00的记录记为当日日期，4:00之前的日期指定为前一天的日期
    * 将此日期添加到DataFrame，并将其他列删除
    * @return self
    */
  def addDate(): DataClean = {
    val time2date = udf{(time: String) => time.split(" ")(0)}
    val addStamp = this.data.withColumn("dateStamp", unix_timestamp(col("cardTime"), "yyyy-MM-dd HH:mm:ss"))
    val addDate = addStamp.withColumn("oldDate", time2date(col("cardTime"))) //旧日期
    val addBeginTime = addDate.withColumn("beginTime", unix_timestamp(col("oldDate"), "yyyy-MM-dd") + 60 * 60 * 4)
    val addEndTime = addBeginTime.withColumn("endTime", unix_timestamp(col("oldDate"), "yyyy-MM-dd") + 60 * 60 * 28)
    val addNewDate = addEndTime.withColumn("date", when(col("dateStamp") > col("beginTime") && col("endTime") > col("dateStamp"), col("oldDate"))
      .otherwise(date_format((col("dateStamp") - 60 * 60 * 24).cast("timestamp"), "yyyy-MM-dd")))
      .drop("dateStamp", "oldDate", "beginTime", "endTime")
    newUtils(addNewDate)
  }

  /**
    * 针对地铁数据做站点和地铁线路补全
    * 根据站点ID唯一对应站点名称，利用站点ID补全站点名称为“None”的字段，统一所有站点名称
    * 根据站点ID唯一确定路线名称，利用站点ID修正路线名称错误的字段
    * @param dataStation 静态地铁站点数据
    * @return 原对象DataCleanUtils
    */
  def recoveryData(dataStation: Broadcast[Map[String, Array[(String, String, String, String)]]]): DataClean = {
    val newData = this.data.map(row => {
      val siteId = row.getString(row.fieldIndex("terminalCode")).substring(0, 6) //getString(i):以 string 类型返回i 对应的值
      val flag = siteId.matches("2[46].*")
      var siteName = row.getString(row.fieldIndex("siteName"))

      if (flag) {
        siteName = dataStation.value.getOrElse(siteId, Array()).head._2
      }
      (row.getString(row.fieldIndex("cardCode")), row.getString(row.fieldIndex("cardTime")), row.getString(row.fieldIndex("tradeType")),  row.getDouble(row.fieldIndex("trulyTradeValue")), row.getString(row.fieldIndex("terminalCode")), row.getString(row.fieldIndex("routeName")), siteName, row.getString(row.fieldIndex("date")))
    }).toDF("cardCode", "cardTime", "tradeType", "trulyTradeValue", "terminalCode", "routeName", "siteName", "date")
    newUtils(newData)
  }
}

object DataClean {
  def apply(data: DataFrame): DataClean = new DataClean(data)
}
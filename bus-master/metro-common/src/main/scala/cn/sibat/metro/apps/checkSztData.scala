package cn.sibat.metro.apps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}

/**
  * 检验深圳通数据的准确性
  * Created by wing on 2017/9/22.
  */
object CheckSztData {
    def main(args: Array[String]): Unit = {

        var oldDataPath = "/user/wuying/SZT_original/oldData"
        var newDataPath = "/user/wuying/SZT_original/newData"
        var staticMetroPath = "/user/wuying/metroInfo/subway_station"
        if (args.length == 3) {
            oldDataPath = args(0)
            newDataPath = args(1)
            staticMetroPath = args(2)
        } else System.out.println("使用默认的参数配置")

        val spark = SparkSession.builder()
//            .appName("CarFreeDayAPP")
//            .master("local[3]")
//            .config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "cn.sibat.metro.MyRegistrator")
            .config("spark.rdd.compress", "true")
            .getOrCreate()
        import spark.implicits._
        val cardCode1 = "330179182"
//        val cardCode2 = "684448326"
        CarFreeDay.apply.getData(spark, oldDataPath, newDataPath)
            .filter(col("cardCode")===cardCode1)
//            .filter(row => row.getString(row.fieldIndex("terminalCode")).matches("2[235].*"))
            .map(_.mkString(","))
//            .show()
            .repartition(1)
            .rdd
            .saveAsTextFile("checkRecords")

        spark.stop()
    }
}

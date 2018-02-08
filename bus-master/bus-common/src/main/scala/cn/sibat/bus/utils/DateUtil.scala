package cn.sibat.bus.utils

import java.text.SimpleDateFormat

/**
  * Created by kong on 2017/7/18.
  */
object DateUtil {
    /**
      * 两个时间点之间的差值
      *
      * @param firstTime 前一时间
      * @param lastTime 后一时间
      * @return 时间差
      */
    def dealTime(firstTime: String, lastTime: String): Double = {
      var result = -1.0
      try {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        result = (sdf.parse(lastTime).getTime - sdf.parse(firstTime).getTime).toDouble /1000
      }catch {
        case e:Exception => e.printStackTrace()
      }
      result
    }
}

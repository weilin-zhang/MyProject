
import cn.sibat.truck.ParseShp

import scala.util.matching.Regex

/**
  * Created by wing on 2017/10/10.
  */
object parseShpTest {
    def main(args: Array[String]): Unit = {
        val shpPath = args(0)
        val result = ParseShp(shpPath).getZoneName(120,11)
        println(result)
    }
}

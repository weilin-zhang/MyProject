package com.kgj

import java.sql.Timestamp
import java.util

import cn.sibat.kgj.CleanBolt
import cn.sibat.kgj.CleanBolt.{busid, buscompany, busrange}

import org.apache.spark.sql.Dataset

/**
  * 客运车GPS数据基本格式
  *
  * @param carId 车牌号
  * @param lon 经度
  * @param lat 纬度
  * @param time 时间
  * @param deviceNum 设备号
  * @param speed 速度
  * @param color 颜色
  */
case class KGJ_GPS(carId: String, lon: Double, lat: Double, time: String, deviceNum: String, speed: Int, color: String)

/**
  * 客运车越界警告数据
  * @param warningTimeStart 警告开始时间
  * @param warningTimeEnd 警告结束时间
  * @param carId 车牌号
  * @param deviceNum 设备号
  * @param lonStart 越界开始经度
  * @param latStart 越界开始纬度
  * @param lonEnd 越界结束经度
  * @param latEnd 越境结束纬度
  * @param color 颜色
  * @param compName 公司名称
  * @param area 车辆所属辖区
  * @param speed 速度
  * @param warningType 警告类型：4为省外越界警告类型，6为市外越界警告类型
  */
case class GPSWarning(warningTimeStart: Timestamp, warningTimeEnd: Timestamp, carId: String, deviceNum: String,
                      lonStart: Double, latStart: Double, lonEnd: Double, latEnd: Double, color: String,
                      compName: String, area: String, speed: Int, warningType: Int)

/**
  * 使用客管局数据对车辆越界进行预警，
  * 将storm流式处理方式改为spark批处理月报
  * Created by wing1995 on 2017/8/8.
  */
object KGJDataWarning{

  private[kgj] val guangdong = new util.HashMap[String, String]
  private[kgj] val shenzhen = new util.HashMap[String, String]

  def vehicleWarning(data: Dataset[String], savePath: String): Unit = {
    import data.sparkSession.implicits._
    new CleanBolt

//    data.map(line => {
//      val arr = line.split(",")
//      KGJ_GPS(arr(0), arr(1).toDouble, arr(2).toDouble, arr(3), arr(4), arr(5).toInt, arr(11))
//    })
//      .toDF()
//      .groupByKey(row => row.getString(row.fieldIndex("carId")))
//      .flatMapGroups((id, iter) => {
//        val records = iter.toArray
//        records.sortBy(row => row.getString(row.fieldIndex("time")))
//          .foreach(row => {
//            val id = row.getString(row.fieldIndex("carId"))
//            //若当前车辆ID对应的公司名称已存在散列表中，则依次获取对应的公司名称以及管辖区域和车辆类型
//            if (busrange.get(id) != null) {
//              val company = busid.get(id)
//              val area = buscompany.get(company)
//              val range = busrange.get(id)
//              val lon = Double.valueOf(st1(1)).doubleValue
//              val lat = Double.valueOf(st1(2)).doubleValue
//              val time = st1(3)
//              val speed = st1(5).toInt
//              //每天19点情况散列表，重新连接数据库并更新散列表内容
//              val date = new Date
//              val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//              val now = format.format(date)
//              val regex1 = ".* 19:00:00"
//              val pattern1 = Pattern.compile(regex1)
//              val matcher1 = pattern1.matcher(now)
//              if (matcher1.find) busid.clear()
//              //若当前时间为19点则清空busid散列表
//              //继续解析GPS数据
//              val device = st1(4)
//              val direction = st1(6)
//              val pstatus = st1(7).toInt
//              val warning = st1(8).toInt
//              val simnum = st1(9)
//              val color = st1(11)
//              //判断车辆类型，省外越界报警类型是4，市外越界报警类型是6
//              val `type` = 4
//              val type1 = 6
//              val out = ss1
//              val regex2 = ".*省际包车客运.*"
//              val pattern2 = Pattern.compile(regex2)
//              val matcher2 = pattern2.matcher(range)
//              val regex3 = ".*市际包车客运.*"
//              val pattern3 = Pattern.compile(regex3)
//              val matcher3 = pattern3.matcher(range)
//              val regex4 = ".*县际包车客运.*"
//              val pattern4 = Pattern.compile(regex4)
//              val matcher4 = pattern4.matcher(range)
//              val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
//              val reader = new WKTReader(geometryFactory)
//              val coord = new Coordinate(lon, lat)
//              val point = geometryFactory.createPoint(coord)
//              //将经纬度合成一个坐标系中的点
//              var s0 = null
//              var s1 = null
//              var s2 = null
//              //读本地测试文件，判断GPS数据是否越界
//              try
//                //s1 = readfile("/home/cairuxin/hongkong.txt");
//                s0 = readfile("hongkong.txt")
//                //香港的管辖范围数据
//                val polygon1 = reader.read(s0).asInstanceOf[Polygon]
//                //s1 = readfile("/home/cairuxin/guangdong.txt");
//                s1 = readfile("guangdong.txt")
//                //广东的管辖范围数据
//                val polygon = reader.read(s1).asInstanceOf[Polygon]
//                //若干当前车辆位置不在广东也不在香港，并且车辆类型属于市级包车客运，可判断当前车辆省外越界
//                if (!polygon.contains(point) && !polygon1.contains(point) && matcher3.find) {
//                  //若当前车辆ID没有越界记录则更新越界范围guangdong散列表和越界开始时间starttime散列表，并将越界记录插入到数据库中且输出该车量行驶轨迹数据
//                  if (guangdong.get(id) ne "b") {
//                    val timesql = java.sql.Timestamp.valueOf(time)
//                    val tosql = new cleanBolt
//                    tosql.insert(new Nothing(timesql, timesql, id, device, lon, lat, lon, lat, color, company, area, speed, `type`))
//                    guangdong.put(id, "b")
//                    starttime.put(id, time)
//                    collector.emit(new Nothing(busrange.get(id) + "，省外越界，" + out))
//                    System.out.println(busrange.get(id) + "，省外越界，" + out)
//                  }
//                  else {
//                    //若有该车的违规记录，则更新该车的省外越界记录
//                    val updatesql = new cleanBolt
//                    val timesql = java.sql.Timestamp.valueOf(time)
//                    val timesql1 = java.sql.Timestamp.valueOf(starttime.get(id))
//                    updatesql.update(new Nothing(timesql1, timesql, id, device, lon, lat, lon, lat, color, company, area, speed, `type`))
//                    collector.emit(new Nothing(busrange.get(id) + "，省外越界更新，" + out))
//                    System.out.println(busrange.get(id) + "，省外越界更新，" + out)
//                  }
//                }
//                else guangdong.put(id, "a")
//                //读取深圳市的版图数据
//                //s2 = readfile("/home/cairuxin/shenzhen1.txt");
//                s2 = readfile("shenzhen.txt")
//                val polygon0 = reader.read(s2).asInstanceOf[Polygon]
//                //若当前车辆位置既不在深圳又不在香港，且该车类型属于县级包车，则认为该车市外越界，并更新深圳市外越界的散列表shenzhen和越界开始时间starttime
//                if (!polygon0.contains(point) && !polygon1.contains(point) && matcher4.find) if (shenzhen.get(id) ne "b") {
//                  val timesql = java.sql.Timestamp.valueOf(time)
//                  val timesql1 = java.sql.Timestamp.valueOf(time)
//                  val tosql = new cleanBolt
//                  starttime.put(id, time)
//                  shenzhen.put(id, "b")
//                  tosql.insert(new Nothing(timesql1, timesql, id, device, lon, lat, lon, lat, color, company, area, speed, type1))
//                  collector.emit(new Nothing(busrange.get(id) + "，市外越界 ，" + out))
//                  System.out.println(busrange.get(id) + "，市外越界 ，" + out)
//                }
//                else {
//                  val updatesql = new cleanBolt
//                  val timesql = java.sql.Timestamp.valueOf(time)
//                  val timesql1 = java.sql.Timestamp.valueOf(starttime.get(id))
//                  updatesql.update(new Nothing(timesql1, timesql, id, device, lon, lat, lon, lat, color, company, area, speed, type1))
//                  collector.emit(new Nothing(busrange.get(id) + "，市外越界更新 ， " + out))
//                  System.out.println(busrange.get(id) + "，市外越界更新 ， " + out)
//                }
//                else shenzhen.put(id, "a")
//
//              catch {
//                case e: Exception => {
//                  // TODO Auto-generated catch block
//                  e.printStackTrace()
//                }
//              }
//            }
//          })
//      })
  }

  def main(args: Array[String]): Unit = {

  }
}

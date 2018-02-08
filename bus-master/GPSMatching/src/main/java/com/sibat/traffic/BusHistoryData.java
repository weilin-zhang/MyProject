package com.sibat.traffic;

import util.*;

import java.io.*;

/**
 * Created by User on 2017/5/23.
 * 配置好文件后，可以根据输入的文件夹地址循环处理原始Bus GPS记录，并输出处理后的数据
 */
public class BusHistoryData {
    static BufferedWriter bw;

    public static void main(String[] args) throws FileNotFoundException {
        DealDataBolt dealBolt = new DealDataBolt();
        String fileName = args[0].toString();
//        String fileName = "D:\\data\\data\\bus_data\\STRING_20160103";

        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("file is not exits");
            return;
        }

        FileReader fileReader = new FileReader(fileName);

        String str;
        // Open the reader
        BufferedReader reader = new BufferedReader(fileReader);
        String fileNames[] = fileName.split("/");
//      String fileNames[] = fileName.split("\\\\");
        GlobalInfo.writeFileName = fileNames[fileNames.length - 1];
        GlobalInfo.outPath = Cfg.pathMap.get(Cfg.BUS_OUT);

        //初始化，得到写文件的句柄
        bw= WriteFile.initBufferedWriter(GlobalInfo.writeFileName, GlobalInfo.outPath);

        // Read all lines
        String vehicletype = "bus", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "bus";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;

        long start=System.currentTimeMillis();
        try {
            // Read all lines
            while ((str = reader.readLine()) != null) {
                try {
                    //系统时间、类型、终端号、车牌号、线路id、子线路id、公司、状态、经度、纬度、高度、GPS上报时间、定位速度、方向、行车记录仪速度、里程
                    lineItem = str.split(",");
                    devicetime = StringUtil.getDevicetime(DataSourceCfg.INDEX_BUS_DEVICETIME, lineItem);//补齐为完整的日期格式
                    if (devicetime == null) {
                        System.out.println("解析错误!!!");
                        continue;
                    }
                    String[] date_time = devicetime.split(" ");
                    String time = date_time[1];
                    String[] h_m_s = time.split(":");
                    String h = h_m_s[0];
                    int hour = Integer.parseInt(h);
                    if (23 < hour || hour < 6) // 特殊时间，摒弃处理
                    {
                        continue;
                    }
                    timestamp = StringUtil.getTimestamp(devicetime);

                    vehicleid = StringUtil.getVehicleid(DataSourceCfg.INDEX_BUS_VEHICLEID, lineItem);
                    x = StringUtil.getLng(DataSourceCfg.INDEX_BUS_LNG, lineItem);
                    y = StringUtil.getLat(DataSourceCfg.INDEX_BUS_LAT, lineItem);

                    //wgs2gcj
                    double[] gcj = GPS.wgs2gcj(y, x);
                    float lng = (float) gcj[1];
                    float lat = (float) gcj[0];

                    speed = StringUtil.getSpeed(DataSourceCfg.INDEX_BUS_SPEED, lineItem);
                    direction = StringUtil.getDirection(DataSourceCfg.INDEX_BUS_DIRECTION, lineItem);

                    Record info = new Record(vehicletype, vehicleid, lng, lat,
                            timestamp, deviceid, speed, direction, extrainfo);
                    dealBolt.dealData(info, bw);
                } catch (Exception e) {
                    continue;
                }
            }
            System.out.println("总共耗时:-------------------------------->" + (System.currentTimeMillis() - start) / 1000 + "s");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

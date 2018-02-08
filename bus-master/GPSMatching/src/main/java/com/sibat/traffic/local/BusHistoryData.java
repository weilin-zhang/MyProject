package com.sibat.traffic.local;

import com.sibat.traffic.GlobalInfo;
import util.DataSourceCfg;
import util.GPS;
import util.StringUtil;
import util.WriteFile;

import java.io.*;

/**
 * Created by User on 2017/5/23.
 */
public class BusHistoryData {
    static BufferedWriter bw;

    public static void main(String[] args) throws FileNotFoundException {
        LocalDealDataBolt dealBolt = new LocalDealDataBolt();
        String fileName = LocalPathUtil.busPath;

        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("file is not exits");
            return;
        }

        FileReader fileReader = new FileReader(fileName);

        String str;
        // Open the reader
        BufferedReader reader = new BufferedReader(fileReader);
        String fileNames[] = fileName.split("\\\\");
        GlobalInfo.writeFileName = fileNames[fileNames.length - 1];
        GlobalInfo.outPath = LocalPathUtil.outbusPath;

        //初始化，得到写文件的句柄
        bw = WriteFile.initBufferedWriter(GlobalInfo.writeFileName, GlobalInfo.outPath);

        // Read all lines
        String vehicletype = "bus", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "bus";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;

        long start = System.currentTimeMillis();
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

                    com.sibat.traffic.Record info = new com.sibat.traffic.Record(vehicletype, vehicleid, lng, lat,
                            timestamp, deviceid, speed, direction, extrainfo);
                    dealBolt.dealData(info, bw);
                } catch (Exception e) {
                    continue;
                }
            }
            System.out.println("总共耗时:-------------------------------->" + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

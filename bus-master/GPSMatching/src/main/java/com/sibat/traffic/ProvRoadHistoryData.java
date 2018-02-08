package com.sibat.traffic;

import util.*;

import java.io.*;
import java.text.ParseException;

/**
 * Created by User on 2017/5/23.
 *  * 配置好文件后，可以根据输入的文件夹地址循环处理原始ProvRoad GPS记录，并输出处理后的数据
 */
public class ProvRoadHistoryData {
    private static FileReader fileReader;

    static BufferedWriter bw;

    public static void main(String[] args) throws FileNotFoundException {
        DealDataBolt dealBolt = new DealDataBolt();
        String fileName = args[0].toString();
        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("file is not exits");
            return;
        }
        fileReader = new FileReader(fileName);
        BufferedReader reader = new BufferedReader(fileReader);
        String fileNames[] = fileName.split("/");
        GlobalInfo.writeFileName = fileNames[fileNames.length - 1];
        GlobalInfo.outPath = Cfg.pathMap.get(Cfg.PROVROAD_OUT);

        //初始化，得到写文件的句柄
        bw = WriteFile.initBufferedWriter(GlobalInfo.writeFileName, GlobalInfo.outPath);

        String str;
        String vehicletype = "coach", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "ProvRoad";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;
        try {
            // Read all lines
            while ((str = reader.readLine()) != null) {
                try {
                    //系统时间、城市编码，车牌号、颜色、GPS上报时间、经度、纬度、速度、方向、？、？、？
                    lineItem = str.split(",");
//                0->2016-01-01 00:00:00
//                1->A
//                2->粤AE4386
//                3->2
//                4->2015-12-31 23:59:52
//                5->22.991766666666667
//                6->113.27958333333333
//                7->40
//                8->146
//                9->7
//                10->1947911445
//                11->000000000000
//                12->000000000000000000
//                13->1
//                14->0320000000000000
                    //"vehicletype","vehicleid","x","y","devicetime","deviceid","speed","direction","extrainfo"
                    vehicleid = StringUtil.getVehicleid(DataSourceCfg.INDEX_PROVROAD_VEHICLEID, lineItem);
                    x = StringUtil.getLng(DataSourceCfg.INDEX_PROVROAD_LNG, lineItem);
                    y = StringUtil.getLat(DataSourceCfg.INDEX_PROVROAD_LAT, lineItem);

                    //wgs2gcj
                    double[] gcj = GPS.wgs2gcj(y, x);
                    float lng = (float) gcj[1];
                    float lat = (float) gcj[0];

                    timestamp = 0;
                    try {
                        devicetime = StringUtil.getDevicetime(DataSourceCfg.INDEX_PROVROAD_DEVICETIME, lineItem);//2015-12-31 23:59:52
                        timestamp = StringUtil.getTimestamp(devicetime);
                    } catch (ParseException e) {
                        //e.printStackTrace();
                        continue;
                    }
                    speed = Float.parseFloat(lineItem[7]);
                    direction = Float.parseFloat(lineItem[8]);


                    Record info = new Record(vehicletype, vehicleid, lng, lat,
                            timestamp, deviceid, speed, direction, extrainfo);
                    dealBolt.dealData(info, bw);
                } catch (Exception e) {
                    continue;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

package com.sibat.traffic.local;

import com.sibat.traffic.GlobalInfo;
import util.DataSourceCfg;
import util.GPS;
import util.StringUtil;
import util.WriteFile;

import java.io.*;
import java.text.ParseException;

/**
 * Created by User on 2017/5/23.
 */
public class TaxiHistoryData {
    private static FileReader fileReader;
    public static String writefileName = "";

    static BufferedWriter bw;

    public static void main(String[] args) throws FileNotFoundException {
        LocalDealDataBolt dealBolt = new LocalDealDataBolt();
        String str;
        // Open the reader
        String fileName = LocalPathUtil.taxiPath;
        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("file is not exits");
            return;
        }
        fileReader = new FileReader(fileName);
        BufferedReader reader = new BufferedReader(fileReader);
        String fileNames[] = fileName.split("\\\\");
        GlobalInfo.writeFileName = fileNames[fileNames.length - 1];
        GlobalInfo.outPath = LocalPathUtil.outtaxiPath;

        bw = WriteFile.initBufferedWriter(GlobalInfo.writeFileName, GlobalInfo.outPath);

        String vehicletype = "taxi", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "taxi";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;
        try {
            // Read all lines
            while ((str = reader.readLine()) != null) {
                try {
                    //车牌号、经度、纬度、上报时间、设备号、速度、方向、定位状态、报警类型、SIM卡号、载客状态、车牌颜色
                    lineItem = str.split(",");
                    //                0->粤B0V3P6
                    //                1->114.122849
                    //                2->22.579933
                    //                3->2016-01-01 00:05:12
                    //                4->1568616
                    //                5->9
                    //                6->180
                    //                7->0
                    //                8->
                    //                9->
                    //                10->0
                    //                11->蓝色
                    //"vehicletype","vehicleid","x","y","devicetime","deviceid","speed","direction","extrainfo"
                    vehicleid = StringUtil.getVehicleid(DataSourceCfg.INDEX_TAXI_VEHICLEID, lineItem);
                    x = StringUtil.getLng(DataSourceCfg.INDEX_TAXI_LNG, lineItem);
                    y = StringUtil.getLat(DataSourceCfg.INDEX_TAXI_LAT, lineItem);

                    //wgs2gcj
                    double[] gcj = GPS.wgs2gcj(y, x);
                    float lng = (float) gcj[1];
                    float lat = (float) gcj[0];

                    timestamp = 0;
                    try {
                        devicetime = StringUtil.getDevicetime(DataSourceCfg.INDEX_TAXI_DEVICETIME, lineItem);//2016-01-01 00:05:12
                        timestamp = StringUtil.getTimestamp(devicetime);
                    } catch (ParseException e) {
                        continue;
                    }
                    deviceid = StringUtil.getDeviceid(DataSourceCfg.INDEX_TAXI_DEVICEID, lineItem);
                    speed = StringUtil.getSpeed(DataSourceCfg.INDEX_TAXI_SPEED, lineItem);
                    direction = StringUtil.getDirection(DataSourceCfg.INDEX_TAXI_DIRECTION, lineItem);
                    com.sibat.traffic.Record info = new com.sibat.traffic.Record(vehicletype, vehicleid, lng, lat,
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

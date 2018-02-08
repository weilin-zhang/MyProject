package com.sibat.traffic;

import util.*;

import java.io.*;
import java.text.ParseException;

/**
 * Created by User on 2017/5/19.
 *  * 配置好文件后，可以根据输入的文件夹地址循环处理原始E6 GPS记录，并输出处理后的数据
 */
public class E6HistioryData {
    private static FileReader fileReader;
    public static String writefileName = "";

    static BufferedWriter bw;

    public static void main(String[] args) throws IOException {
        DealDataBolt dealBolt = new DealDataBolt();
        //  /home/datum/storm/data/GPS/e6_data/STRING_20160103
        String fileName = args[0].toString();
        File file = new File(fileName);
        if(!file.exists()){
            System.out.println("file is not exits");
            return ;
        }

        String str = "";
        String vehicletype = "truck", vehicleid = null, devicetime = null, deviceid = null, extrainfo = "E6";
        float x = 0, y = 0, speed = 0, direction = 0;
        long timestamp = 0;
        String[] lineItem;
        BufferedReader reader = null;
        try {
            fileReader = new FileReader(fileName);
            reader = new BufferedReader(fileReader);
            String fileNames[] = fileName.split("/");
            GlobalInfo.writeFileName =  fileNames[fileNames.length-1];
            System.out.println("out file name =>"+ GlobalInfo.writeFileName);
            GlobalInfo.outPath =  Cfg.pathMap.get(Cfg.E6_OUT);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //初始化，得到写文件的句柄
        bw= WriteFile.initBufferedWriter(GlobalInfo.writeFileName, GlobalInfo.outPath);

        while ((str = reader.readLine()) != null) {
           // str = reader.readLine();
            try {
                lineItem = str.split(",");
                if (lineItem.length != 9)
                    continue;
                //"vehicletype","vehicleid","x","y","devicetime","deviceid","speed","direction","extrainfo"
                vehicleid = lineItem[0];
                if (lineItem[1].toString().equals("\\") || lineItem[2].toString().equals("\\"))//存在一些非法的string 在转化之前进行过滤操作
                {
                    lineItem[1] = "0.000";
                    lineItem[2] = "0.000";
                }
                x = StringUtil.getLng(DataSourceCfg.INDEX_E6_LNG, lineItem) / 1000000;
                y = StringUtil.getLat(DataSourceCfg.INDEX_E6_LAT, lineItem) / 1000000;

                //wgs2gcj
                double[] gcj = GPS.wgs2gcj(y,x);
                float lng = (float) gcj[1];
                float lat = (float) gcj[0];

                speed = StringUtil.getSpeed(DataSourceCfg.INDEX_E6_SPEED,lineItem);
                direction = StringUtil.getDirection(DataSourceCfg.INDEX_E6_DIRECTION,lineItem);
                try {
                    devicetime = StringUtil.getDevicetime(DataSourceCfg.INDEX_E6_DEVICETIME,lineItem);//补齐为完整的日期格式
                    timestamp = StringUtil.getTimestamp(devicetime);//毫秒
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                Record info = new Record(vehicletype, vehicleid, lng, lat,
                        timestamp, deviceid, speed, direction, extrainfo);
                dealBolt.dealData(info, bw);
            } catch (NumberFormatException e) {
                System.out.println("[ERROR]NumberFormatException");
                continue;
            }

        }


    }


}

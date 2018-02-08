package com.sibat.traffic.local;

/**
 * Created by nanphonfy on 2017/8/10.
 */
public class LocalPathUtil {
    public final static String basePath = System.getProperty("user.dir") + "\\src\\main\\resources\\data";

    public final static String busPath = basePath+"\\bus_data\\bus_data.txt";
    public final static String coachPath = basePath+"\\coach_data\\coach_data.txt";
    public final static String e6Path = basePath+"\\e6_data\\e6_data.txt";
    public final static String taxiPath = basePath+"\\taxi_data\\taxi_data.txt";

    public final static String outbusPath = basePath+"\\out\\bus";
    public final static String outcoachPath = basePath+"\\out\\coach";
    public final static String oute6Path = basePath+"\\out\\e6";
    public final static String outtaxiPath = basePath+"\\out\\taxi";

    public final static String shpPath = basePath+"\\guangdong_polyline\\guangdong_polyline.shp";
}

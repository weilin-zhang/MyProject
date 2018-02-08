package util;

import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * @author zhengshaorong
 * 数据配置
 */
public class DataSourceCfg {
    public static final String INDEX_BUS_SYSTEMTIME = "index.bus.systemtime";
    //	public static final String INDEX_BUS_VEHICLETYPE = "index.bus.vehicletype";
    public static final String INDEX_BUS_VEHICLEID = "index.bus.vehicleid";
    public static final String INDEX_BUS_LNG = "index.bus.lng";
    public static final String INDEX_BUS_LAT = "index.bus.lat";
    public static final String INDEX_BUS_DEVICETIME = "index.bus.devicetime";
    public static final String INDEX_BUS_DEVICEID = "index.bus.deviceid";
    public static final String INDEX_BUS_SPEED = "index.bus.speed";
    public static final String INDEX_BUS_DIRECTION = "index.bus.direction";
    //	public static final String INDEX_BUS_EXTRAINFO = "index.bus.extrainfo";

    public static final String INDEX_TAXI_SYSTEMTIME = "index.taxi.systemtime";
    public static final String INDEX_TAXI_VEHICLEID = "index.taxi.vehicleid";
    public static final String INDEX_TAXI_LNG = "index.taxi.lng";
    public static final String INDEX_TAXI_LAT = "index.taxi.lat";
    public static final String INDEX_TAXI_DEVICETIME = "index.taxi.devicetime";
    public static final String INDEX_TAXI_DEVICEID = "index.taxi.deviceid";
    public static final String INDEX_TAXI_SPEED = "index.taxi.speed";
    public static final String INDEX_TAXI_DIRECTION = "index.taxi.direction";
    
    public static final String INDEX_E6_SYSTEMTIME = "index.e6.systemtime";
    public static final String INDEX_E6_VEHICLEID = "index.e6.vehicleid";
    public static final String INDEX_E6_LNG = "index.e6.lng";
    public static final String INDEX_E6_LAT = "index.e6.lat";
    public static final String INDEX_E6_DEVICETIME = "index.e6.devicetime";
    public static final String INDEX_E6_DEVICEID = "index.e6.deviceid";
    public static final String INDEX_E6_SPEED = "index.e6.speed";
    public static final String INDEX_E6_DIRECTION = "index.e6.direction";
    
    public static final String INDEX_PROVROAD_SYSTEMTIME = "index.provroad.systemtime";
    public static final String INDEX_PROVROAD_VEHICLEID = "index.provroad.vehicleid";
    public static final String INDEX_PROVROAD_LNG = "index.provroad.lng";
    public static final String INDEX_PROVROAD_LAT = "index.provroad.lat";
    public static final String INDEX_PROVROAD_DEVICETIME = "index.provroad.devicetime";
    public static final String INDEX_PROVROAD_DEVICEID = "index.provroad.deviceid";
    public static final String INDEX_PROVROAD_SPEED = "index.provroad.speed";
    public static final String INDEX_PROVROAD_DIRECTION = "index.provroad.direction";

    public static Map<String, String> typeDataSourceMap = new HashMap<String, String>();

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("datasource");
        Enumeration<String> eum = bundle.getKeys();
        while (eum.hasMoreElements()) {
            String key = eum.nextElement();
            try {
                typeDataSourceMap.put(key, new String(bundle.getString(key).getBytes("ISO-8859-1"), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }
}

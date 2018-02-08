package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * String处理工具
 */
public class StringUtil {
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 如果设备号时间不是2017格式，而是17格式，补全年份
     *
     * @param devicetimeKey 设备时间的键
     * @param lineItem      传入读取行数组
     * @return
     */
    public static String getDevicetime(String devicetimeKey, String[] lineItem) {
        String str = DataSourceCfg.typeDataSourceMap.get(devicetimeKey);
        if (str.contains("lack")) {
            String[] arr = str.split(",");
            int index = getInt(arr[0], -1);
            int prefix = getInt(arr[1].replace("lack", ""), -1);
            if (index == -1 || prefix == -1) {
                return null;
            }
            return prefix + lineItem[index];
        } else {
            int index = getInt(str, -1);
            if (index == -1) {
                return null;
            }
            return lineItem[index];
        }
    }

    public static long getTimestamp(String devicetime) throws ParseException {
        return df.parse(devicetime).getTime() / 1000;
    }

    public static String getVehicleid(String vehicleidKey, String[] lineItem) {
        String str = DataSourceCfg.typeDataSourceMap.get(vehicleidKey);
        return lineItem[getInt(str, -1)];
    }

    public static float getLng(String lngKey, String[] lineItem) {
        String str = DataSourceCfg.typeDataSourceMap.get(lngKey);
        return Float.parseFloat(lineItem[getInt(str, -1)]);
    }

    public static float getLat(String latKey, String[] lineItem) {
        String str = DataSourceCfg.typeDataSourceMap.get(latKey);
        return Float.parseFloat(lineItem[getInt(str, -1)]);
    }

    public static float getSpeed(String speedKey, String[] lineItem) {
        String str = DataSourceCfg.typeDataSourceMap.get(speedKey);
        return Float.parseFloat(lineItem[getInt(str, -1)]);
    }

    public static float getDirection(String directionKey, String[] lineItem) {
        String str = DataSourceCfg.typeDataSourceMap.get(directionKey);
        return Float.parseFloat(lineItem[getInt(str, -1)]);
    }

    public static String getDeviceid(String deviceidKey, String[] lineItem) {
        String str = DataSourceCfg.typeDataSourceMap.get(deviceidKey);
        return lineItem[getInt(str, -1)];
    }

    /**
     * 把字符型数字转换成整型.
     *
     * @param str 字符型数字
     * @return int 返回整型值。如果不能转换则返回默认值defaultValue.
     */
    public static int getInt(String str, int defaultValue) {
        if (str == null)
            return defaultValue;
        if (isInt(str)) {
            return Integer.parseInt(str);
        } else {
            return defaultValue;
        }
    }

    /**
     * 判断一个字符串是否为数字
     */
    public static boolean isInt(String str) {
        return str.matches("\\d+");
    }

    public static double getDouble(String str, double defaultValue) {
        if (str == null)
            return defaultValue;
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static long getLong(String str, long defaultValue) {
        if (str == null)
            return defaultValue;
        try {
            return Long.parseLong(str);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}

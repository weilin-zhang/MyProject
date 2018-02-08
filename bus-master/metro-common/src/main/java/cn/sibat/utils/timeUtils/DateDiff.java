package cn.sibat.utils.timeUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * java的日期差在scala中会出错
 * Created by wing on 2017/9/20.
 */
public class DateDiff {
    public static int getDaysDiff(String oldDate, String newDate)
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        int days = -1;
        try
        {
            Date oldDateStamp = format.parse(oldDate);
            Date newDateStamp = format.parse(newDate);
            days =  (int) ((oldDateStamp.getTime() - newDateStamp.getTime()) / (1000*3600*24)) + 1;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return days;
    }
    public static void main(String[] args)
    {
        String dateStr = "2017-07-31";
        String dateStr2 = "2017-07-31";
        System.out.println("两个日期的差距：" + getDaysDiff(dateStr,dateStr2));
    }
}

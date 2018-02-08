package connectToHbase;

import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 各种字符串处理工具
 * Created by wing1995 on 2017/8/13.
 */
public class StringUtil {

    public static String replaceString(String record) {
        String REGEX = "(((?<=,)(0|(0\\.0))(?=,))|((^0|(0\\.0))(?=,))|((?<=,)((0\\.0)|0)$))";
        Pattern p = Pattern.compile(REGEX);
        Matcher m = p.matcher(record);
        return  m.replaceAll("");
    }

    public static String intFormat(Long number) {
        DecimalFormat df = new DecimalFormat("000");
        return df.format(number);
    }

    public static void main(String[] args) {
        String line = "0001097960:17-07-26 16:20:00,1,0.0,0,0.0,1,0.0,0,0.0,0,0.0,0,0.0,0,0.0";
        String arr_value[] = line.split(",", 2);

        String rowKey = arr_value[0];



        String longDate = rowKey.split(":", 2)[1].split(" ")[0];
        String year = longDate.split("-")[0];
        String month = longDate.split("-")[1];
        String day = longDate.split("-")[2];
        String hour = rowKey.split(":", 2)[1].split(" ")[1].split(":")[0];
        String rowKeySuffix = rowKey.split(":", 2)[0] + year + month + day + hour;

        String ss = rowKey.split(":", 2)[0] + year + month + day;

        //System.out.println(Long.parseLong(ss));
        System.out.println(Long.parseLong("285449170725") % 200);

       //String rowKeyPrefix = intFormat(Long.parseLong(rowKey.split(":", 2)[0] + year + month + day) % 200); //道路ID%200并限制三位数用0补全

//        String newRowKey = rowKeyPrefix.concat(rowKeySuffix);
//
//        String newValue = arr_value[1];
//        System.out.println("原字符串记录为：" + line);
//        System.out.println("解析后的rowKey为：" + newRowKey + "，recorder为：" + newValue);
    }
}


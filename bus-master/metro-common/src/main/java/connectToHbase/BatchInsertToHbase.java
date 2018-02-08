package connectToHbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * 单线程批量插入数据
 * Created by wing1995 on 2017/8/6.
 */
public class BatchInsertToHbase {

    private static long startTime;
    private static final int lineCount = 100000; //每次提交时记录的行数
    private static final String address = "192.168.40.49";
    //private static final String tableName = "RoadStat_T3"; //表名

    public static void main(String[] args) throws IOException {

        startTime = System.currentTimeMillis() / 1000;

            try {

                insert_one(args[0], args[1]);

            } catch (IOException e) {

                e.printStackTrace();

            }

    }

    private static void insert_one(String path, String tableName) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", address);

        HTable table = new HTable(conf, tableName);
        table.setAutoFlushTo(false);
        table.setWriteBufferSize(10 * 1024 * 1024);

        ArrayList<Put> list = new ArrayList<>();

        File f = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();

        int count = 0;

        while (line != null) {
            //若提交的列表数目等于指定行，则提交列表；否则往列表里面添加行
            if (list.size() == lineCount) {

                table.put(list);
                table.flushCommits();
                list.clear();
                System.out.println("已插入数据" + count + "条");

            }

            String arr_value[] = line.split(",", 2);

            String rowKey = arr_value[0];

            String longDate = rowKey.split(":", 2)[1].split(" ")[0];
            String year = longDate.split("-")[0];
            String month = longDate.split("-")[1];
            String day = longDate.split("-")[2];
            String hour = rowKey.split(":", 2)[1].split(" ")[1].split(":")[0];
            String rowKeySuffix = rowKey.split(":", 2)[0] + year + month + day + hour;

            DecimalFormat df = new DecimalFormat("000");
            String rowKeyPrefix = "";
            if (tableName.equals("RoadStat_T3")) rowKeyPrefix = df.format(Long.parseLong(rowKey.split(":", 2)[0] + year + month + day) % 200); //道路ID加日期再整除200
            else if (tableName.equals("RoadStat_T4")) rowKeyPrefix = df.format(Long.parseLong(rowKey.split(":", 2)[0]) % 200);//道路ID%200

            String newRowKey = rowKeyPrefix.concat(rowKeySuffix);

            Put p = new Put(newRowKey.getBytes());

            String colNamePrefix = "t";
            Integer minute = Integer.parseInt(rowKey.split(":", 2)[1].substring(12, 14));
            Integer colNameSuffix = minute / 5 + 1;
            String colName = colNamePrefix + colNameSuffix;

            String value = arr_value[1];

            p.add(("recorder").getBytes(), colName.getBytes(), value.getBytes());
            list.add(p);

            line = br.readLine();
            count++;
        }

        //将最后的几行数据添加到表
        if (list.size() > 0) {

            table.put(list);
            table.flushCommits();

        }

        table.close();

        System.out.println("total = " + count);

        long endTime = System.currentTimeMillis() / 1000;
        long costTime = endTime - startTime;

        System.out.println(path + ": cost time = " + costTime + "秒");
    }
}

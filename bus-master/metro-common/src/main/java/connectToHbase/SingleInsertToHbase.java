package connectToHbase;

import java.io.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 往表格里面添加数据
 * Created by wing1995 on 2017/8/2.
 */
public class SingleInsertToHbase {

    /**
     * 功能：Java读取本地文件的内容
     * 1：先获得文件句柄
     * 2：获得文件句柄当做是输入一个字节码流，需要对这个输入流进行读取
     * 3：读取到输入流后，需要读取生成字节流 4：一行一行的输出。readline()。 备注：需要考虑的是异常情况
     *
     * @param filePath 文件路径[本地文件:如： D:\aa.txt]
     */
    private static void readFile(String filePath)
    {
        try
        {
            String encoding = "utf-8";
            File file = new File(filePath);
            // 判断文件是否存在
            if (file.isFile() && file.exists())
            {
                // 考虑到编码格式
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String line = bufferedReader.readLine();

                // 初始化配置
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "192.168.40.49");

                // 初始化hTable，允许批量请求，并将缓存增加到10MB
                HTable hTable = new HTable(conf, "GdRoadStat");

                int count = 0;

                while (line != null)
                {

                    Put put = singleInsertToHbase(line);
                    hTable.put(put);
                    line = bufferedReader.readLine();
                    count ++;

                    if (count % 100000 == 0) {
                        System.out.println("已插入" + count + "条数据");
                    }
                }

                hTable.close();
                bufferedReader.close();
                read.close();
                System.out.println("共有数据" + count + "条");
            }
            else
            {
                System.out.println("找不到指定的文件");
            }
        }
        catch (Exception e)
        {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }

        System.out.println("all data inserted");
    }

    /**
     * hTable方式插入表程序，单条插入
     * @param  record 单条记录
     */
    private static Put singleInsertToHbase(String record){

        String[] keyValueArray = record.split(",",2);
        String rowKey = keyValueArray[0];
        String value = keyValueArray[1];

        // 初始化Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 在对象里面添加即将插入表的元素，包括列族、列名以及元素值
        put.add(Bytes.toBytes("value"), null, Bytes.toBytes(value));

        return put;
    }

    public static void main(String[] args) {

        long start = System.currentTimeMillis() / 1000;

        String filePath = args[0];

        readFile(filePath);

        long end = System.currentTimeMillis() / 1000;

        long time = end - start;

        System.out.println("耗时：" + time + "秒");

    }
}

package connectToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static connectToHbase.Util.contentLimit;

/**
 * 读取文件列表到队列，执行多线程读取文件内容到队列
 * Created by wing1995 on 2017/8/7.
 */

public class Operation {

    long start = System.currentTimeMillis() / 1000;

    private static Configuration hbaseConfig = null;
    private static HTablePool pool = null;
    //private static String tableName = "GdRoadStat";

    private static int count = 0;
    private static LinkedBlockingDeque<File> files;//即将上传的文件队列
    private static ExecutorService UploadThreadPool = Executors.newFixedThreadPool(10);//上传文件线程池

    static{
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.40.49");
        hbaseConfig = HBaseConfiguration.create(HBASE_CONFIG);

        pool = new HTablePool(hbaseConfig, 1000);
    }

    /**
     * 构造器，初始化文件队列
     * 	@param path 需要读取文件的文件夹路径
     */
    public Operation(String path) {

        files = new LinkedBlockingDeque<>();

        ReadFiles(path);

    }

    /**
     * 获取文件列表并将文件添加到files队列
     * @param path 文件夹路径
     */
    private void ReadFiles(String path){

        System.out.println(path);
        File file=new File(path);

        for(int i=0;i<file.listFiles().length;i++){
            files.add(file.listFiles()[i]);
        }

    }

    /**
     * 将文件从队列中获取，并返回到Util工具类进行多线程读文件操作
     */
    public void MultiReadFiles(){

        while(files.size()>0){
            try {

                new Util(files.take());

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public void insertToHbase(String content, String tableName) throws IOException {

        HTableInterface table = pool.getTable(tableName);
        table.setAutoFlushTo(false);
        table.setWriteBufferSize(10 * 1024 * 1024);

        ArrayList<Put> list = new ArrayList<>();

        String[] lineList = content.split("\n");

        for(String line: lineList) {
            //若提交的列表数目等于指定行，则提交列表；否则往列表里面添加行
            if (list.size() == contentLimit) {

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

            count++;
        }

        //将最后的几行数据添加到表
        if (list.size() > 0) {

            table.put(list);
            table.flushCommits();

        }

        table.close();
    }

    /**
     * 多线程处理数据
     */
    public void MultiDeal(final String tableName){

        UploadThreadPool.execute(new Runnable() {
            @Override
            public void run() {

                //若阻塞队列内容为空，则等待读取文件到队列完成
                while (Util.Contents.size() == 0) {
                    try {

                        System.out.println("等待1秒");
                        Thread.sleep(1000);

                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                //若文件内容队列不为空则对文件内容执行操作
                while (Util.Contents.size() > 0) {

                    System.out.println("now numbers:" + Util.Contents.size());
                    String content = null;

                    try {

                        content = Util.Contents.take();
                        //对文件内容进行插入到表操作
                        Operation.this.insertToHbase(content, tableName);

                    } catch (Exception e) {
                        try {

                            e.printStackTrace();
                            //处理失败，内容重回队列，准备再次处理数据
                            Util.Contents.put(content);

                        } catch (InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                }

                System.out.println("now numbers:" + Util.Contents.size());

                if (Util.Contents.size() == 0 && files.size() == 0) {
                    long end = System.currentTimeMillis() / 1000;
                    System.out.println("多线程多客户端插入数据耗时" + (end - start) + "秒");
                    System.exit(0);
                }

            }
        });
    }
}
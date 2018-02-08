package connectToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

public class HBaseMulti {

	private static Configuration hbaseConfig = null;
	private static HTablePool pool = null;
	private static String tableName = "GdRoadTest";
	static{
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.40.49");
		hbaseConfig = HBaseConfiguration.create(HBASE_CONFIG);

		pool = new HTablePool(hbaseConfig, 1000);
	}

    /**
     * 多线程读文件，批量插入到表
     * @param path 文件地址
     * @throws IOException IO出错
     */
	public static void InsertProcess(String path)throws IOException
	{
		long start = System.currentTimeMillis();

        HTableInterface table = pool.getTable(tableName);
		table.setAutoFlushTo(false);
		table.setWriteBufferSize(10 * 1024 * 1024);

        ArrayList<Put> list = new ArrayList<>();

		//读文件，获取数据，批量提交
        File f = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();

        Integer count = 0;
        while (line != null)
        {
            if(list.size() == 100000)
            {
                table.put(list);
                list.clear();
                table.flushCommits();
                System.out.println("已插入数据" + count + "条");
            }

            String arr_value[] = line.split(",", 2);

            String rowKey = arr_value[0];
            String value = arr_value[1];

            Put p = new Put(rowKey.getBytes());

            p.add(("value").getBytes(), "".getBytes(), value.getBytes());
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

        long stop = System.currentTimeMillis();

		System.out.println("线程:" + Thread.currentThread().getId() + "插入数据：" + count + "共耗时：" + (stop - start) * 1.0 / 1000 + "s");
	}


    /**
     * 多线程插入数据
     * @throws InterruptedException 线程中断
     */
	public static void MultThreadInsert() throws InterruptedException
	{
		System.out.println("---------开始MultThreadInsert测试----------");
		long start = System.currentTimeMillis();

		int threadNumber = 10;
		Thread[] threads=new Thread[threadNumber];

		for(int i=0;i<threads.length;i++)
		{
			threads[i]= new ImportThread();
			threads[i].start(); //调用run方法启动新进程
		}

        for (Thread thread : threads) {
            (thread).join();
        }

		long stop = System.currentTimeMillis();

		System.out.println("MultThreadInsert：" + "共耗时："+ (stop - start) * 1.0 / 1000 + "s");
		System.out.println("---------结束MultThreadInsert测试----------");
	}

	public static void main(String[] args)  throws Exception{

		MultThreadInsert();

	}

	public static class ImportThread extends Thread{

		public void run(){
			try{
				InsertProcess("E:\\hbase\\data\\DayData");
			}
			catch(IOException e){
				e.printStackTrace();
			}finally{
				System.gc();
			}
		}

	}

}
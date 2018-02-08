package connectToHbase;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 多线程读取本地文件到阻塞队列的工具类
 * Created by wing1995 on 2017/8/7.
 */
public class Util {

    static LinkedBlockingDeque<String> Contents = new LinkedBlockingDeque<>(15);//内容阻塞队列，最多可存储150个对象
    private static ExecutorService ReadThreadPool = Executors.newFixedThreadPool(10);//读文件到线程池，最多5个线程
    static Integer contentLimit = 100000;

    //全局变量
    private File file;
    BufferedReader reader = null;
    FileInputStream fileInputStream =null;

    /**
     * 构造器
     * @param file 文件地址
     */
    public Util(File file) {
        this.file=file;
        MultiReadFile();
    }

    /**
     * 多线程读文件方法
     */
    private void MultiReadFile(){

        if(file.isFile())
        {
            try {

                fileInputStream = new FileInputStream(file);
                InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
                reader = new BufferedReader(inputStreamReader);
                Util.ReadThreadPool.execute(new MultiReadFile());//开始执行多线程读取文件任务

            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.out.println(file.getName() + "文件不存在");
            }catch(UnsupportedEncodingException e){
                System.out.println(file.getName() + "文件内容格式不支持，请使用UTF-8");
            }
        }

    }

    /**
     * 多线程读文件入阻塞队列的任务实现
     */
    class MultiReadFile implements Runnable{

        @Override
        public void run() {
            //对于需要反复增加的String，使用StringBuild能够提高效率
            StringBuilder lines = new StringBuilder();
            String line;

            int num=0;//行数
            // TODO Auto-generated method stub
            synchronized(reader) {
                try {
                    while((line = reader.readLine()) != null) {

                        num++;
                        lines.append(line).append('\n');

                        if(num > contentLimit){//大于10000行时就存入队列
                            try {

                                Util.Contents.put(lines.toString());

                                lines.delete(0, lines.length());//清空StringBuilder，不要更改为其它清空方法，此方法效率高
                                num = 0; //重新计数

                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }

                    if(num > 0){ //剩下的内容也提交到队列
                        try {

                            Util.Contents.put(lines.toString());
//                            System.out.println(lines.toString());
                            lines.delete(0, lines.length());

                        } catch (InterruptedException e) {

                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            System.out.println("数据没放入队列");

                        }
                    }
                } catch (IOException e) {

                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    System.out.println("读取文件中出了点错误");

                }finally{
                    try {

                        System.out.println("文件"+file.getName()+"读取完毕");
                        reader.close();

                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }
        }

    }
}

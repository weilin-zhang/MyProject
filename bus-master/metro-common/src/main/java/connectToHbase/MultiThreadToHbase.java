package connectToHbase;

/**
 * 多线程读数据到队列，并从队列中取数据多线程插入到表
 * Created by wing1995 on 2017/8/7.
 */
public class MultiThreadToHbase {

    public static void main(String[] args)
    {

        Operation operation = new Operation(args[0]);
        operation.MultiReadFiles(); //多线程读文件
        operation.MultiDeal(args[1]); //多线程处理文件内容
//        File file = new File("E:\\hbase\\data\\DayData");
//        new Util(file);

    }
}

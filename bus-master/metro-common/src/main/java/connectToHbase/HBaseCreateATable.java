package connectToHbase;

import java.io.IOException;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

/**
 * 创建表格
 */
public class HBaseCreateATable {

    private static String HBASE_ADDRESS = "192.168.40.49";
    private static String TABLE_NAME = "GdRoadTest";
    private static String COLUMN_FAMILY = "recorder";

    public static void main(String[] args) throws IOException {

        // Instantiating configuration class
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBASE_ADDRESS);
        final long[] rowCount = {0};
        HBaseAdmin admin = new HBaseAdmin(conf);

        if (!admin.tableExists(TABLE_NAME)) {

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

            admin.createTable(tableDescriptor);

            System.out.println("Table created ");

        } else {
//            // Verifying weather the table is disabled
//            Boolean bool = admin.isTableDisabled(TABLE_NAME);
//
//            if (!bool) admin.disableTable(TABLE_NAME);
//
//            admin.deleteTable(TABLE_NAME);
//            System.out.println("表格已经删除");
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());

            HTable hTable = new HTable(conf, TABLE_NAME);
            ResultScanner resultScanner = hTable.getScanner(scan);

            for (Result result : resultScanner) {
                rowCount[0] += result.size();
            }

            //resultScanner.forEach(result -> rowCount[0] += result.size());

            System.out.println("表的行数为：" + rowCount[0]);
        }
    }
}
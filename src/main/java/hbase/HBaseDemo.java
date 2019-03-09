package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDemo {
    public static Configuration conf;

    static {
        //初始化单例
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","had001");
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isTableExist(tableName)) {
            System.out.println("table " + tableName + " existed");
        } else {
            //创建表描述符 增加列族
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            //创建表
            admin.createTable(descriptor);
            System.out.println("table " + tableName + " create success");
        }
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isTableExist(tableName)) {
            //disable
            admin.disableTable(TableName.valueOf(tableName));
            //drop
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("table " + tableName + " delete success");

        } else {
            System.out.println("table " + tableName + "not existed");
        }
    }

    /**
     * 增加数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        if (isTableExist(tableName)) {
            //新api 获取表
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));

            //put 操作
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
            System.out.println("table " + tableName + " put data success");
        } else {
            System.out.println("table " + tableName + "not existed");
        }
    }

    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName,String...rows) throws IOException {
        if(isTableExist(tableName)){
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));

            //delete 操作
            List<Delete> deleteList = new ArrayList<Delete>();
            for(String row:rows){
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            table.delete(deleteList);
            table.close();
            System.out.println("table " + tableName + " delete multi row success");
        }else {
            System.out.println("table " + tableName + "not existed");
        }
    }

    /**
     * 扫描所有行
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        if(isTableExist(tableName)){
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));

            //扫描
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner){
                Cell[] cells = result.rawCells();
                for(Cell cell:cells){
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));

                }
            }
        }else {
            System.out.println("table " + tableName + "not existed");
        }
    }



    /**
     * 表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    private static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }


    public static void main(String[] args) {
        try {
            System.out.println("start tests");
            System.out.println(isTableExist("student"));
            //createTable("Person","info","job","health");
            addRowData("Person","1001","info","name","nick");
            addRowData("Person","1001","info","sex","male");
            addRowData("Person","1001","info","age","18");

            deleteMultiRow("Person","person");
            getAllRows("Person");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}

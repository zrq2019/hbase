import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import java.io.IOException;

public class HBaseTest {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    static {
        //1.获得Configuration实例并进行相关设置
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.addResource(HBaseTest.class.getResource("hbase-site.xml"));
        //2.获得Connection实例
        try {
            connection = ConnectionFactory.createConnection(configuration);
            //3.1获得Admin接口
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        //创建表student
        String  familyNames[]={"description","course"};
        createTable("student",familyNames);
        //向表中插入001数据
        insert("student","2015001","description","S_Name","Li Lei");
        insert("student","2015001","description","S_Sex","male");
        insert("student","2015001","description","S_Age","23");
        insert("student","2015001","course","Math","86");
        insert("student","2015001","course","English","69");
        //向表中插入002数据
        insert("student","2015002","description","S_Name","Han Meimei");
        insert("student","2015002","description","S_Sex","female");
        insert("student","2015002","description","S_Age","22");
        insert("student","2015002","course","Computer Science","77");
        insert("student","2015002","course","English","99");
        //向表中插入003数据
        insert("student","2015003","description","S_Name","Zhang San");
        insert("student","2015003","description","S_Sex","male");
        insert("student","2015003","description","S_Age","24");
        insert("student","2015003","course","Math","69");
        insert("student","2015003","course","Computer Science","95");

        //创建表2
        String  familyNames2[]={"INFO"};
        createTable("course",familyNames2);
        insert("course","123001","INFO","C_Name","Math");  
        insert("course","123001","INFO","C_Credit","2.0");  
        insert("course","123002","INFO","C_Name","Computer Science"); 
        insert("course","123002","INFO","C_Credit","5.0");
        insert("course","123003","INFO","C_Name","English");
        insert("course","123003","INFO","C_Credit","3.0");

        //查询选修Computer Science的学生的成绩
        search("student");

        //增加新的列族和新列Contact:Email，并增加数据
        insert2("student","Contact");
        insert("student","2015001","Contact","Email","lilie@qq.com");
        insert("student","2015002","Contact","Email","hmm@qq.com");
        insert("student","2015003","Contact","Email","zs@qq.com");

        //删除学号为2015003的学生的选课记录
        delete("student","2015003");

        //删除表
        dropTable("student");
        dropTable("course");
    }
    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * */
    public static void createTable(String tableName, String familyNames[]) throws IOException {
        //如果表存在退出
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table exists!");
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        admin.createTable(tableDescriptor);
        System.out.println("createtable success!");
    }

    /**
     * 删除表
     * @param tableName 表名
     * */
    public static void dropTable(String tableName) throws IOException {
        //如果表不存在报异常
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName+"不存在");
            return;
        }

        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("delete table " + tableName + " ok.");
    }

    /**
     * 指定行/列中插入数据
     * @param tableName 表名
     * @param rowKey 主键rowkey
     * @param family 列族
     * @param column 列
     * @param value 值
     * TODO: 批量PUT
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table =connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("insert record " + rowKey + " to table " + tableName + " ok.");
    }
    public static void insert2(String tableName, String family) throws IOException {  
        //先创建一个列族，才能给这个列族，添加属性
        HColumnDescriptor newFamliy = new HColumnDescriptor(family);
        admin.addColumn(TableName.valueOf(tableName), newFamliy);
        System.out.println("insert family " + family + " to table " + tableName + " ok.");
    }

    /**
     * 删除表中的指定行
     * @param tableName 表名
     * @param rowKey rowkey
     * TODO: 批量删除
     */
    public static void delete(String tableName, String rowKey) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        //Delete delete = new Delete(Bytes.toBytes(rowKey));
        Delete delete = new Delete("course".getBytes());
        table.delete(delete);
        System.out.println("delete " + rowKey + " courses record sucessfully.");
    }

    /**
    * 单条件按查询，查询多条记录
    * @param tableName
    */
    public static void search(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan s = new Scan(); 
        s.setMaxVersions(3);
        s.addColumn("course".getBytes(), "Computer Science".getBytes());
        ResultScanner rs= table.getScanner(s);
        for(Result result:rs){
            System.out.println("Select Computer Science Scores Successfully!");
            for(Cell cell:result.listCells()){ 
                String row = new String(CellUtil.cloneRow(cell));
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(row + ":" + value);
            }
        }
    }
}


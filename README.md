## <font color='midblue'>实验三 - HBase 操作和编程</font>

### 1 实验内容

- 下载并安装 HBase，尝试伪分布模式，集群模式；

- 现有以下关系型数据库中的表和数据，要求以伪分布式运行HBase，编写Java程序将其转换为适合于 HBase 存储的表并插入数据：

  - 设计并创建合适的表；
  - 查询选修Computer Science的学生的成绩；
  - 增加新的列族和新列Contact:Email，并添加数据；
  - 删除学号为2015003的学生的选课记录；
  - 删除所创建的表

- 用 Shell 完成上述 Java 程序的任务。

  

### 2 设计思路

#### 2.1 表的设计

- 关系型数据库中的表

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 195124.jpg)

<img src="C:\Users\THINK\Desktop\屏幕截图 2021-11-19 195148.jpg" style="zoom:93%;" />

<img src="C:\Users\THINK\Desktop\屏幕截图 2021-11-19 195209.jpg" style="zoom:120%;" />

- 适合于 HBase 存储的表

![](C:\Users\THINK\Desktop\F8B7A79978554581008C3DEA563E3224.png)

![](C:\Users\THINK\Desktop\F6CD9AD1C122400E4212A6A2E19D90AC.png)

​         将学生课程选修及得分情况与学生基本信息合并为一张表，课程的学分情况在另一张表中。这样既保证了行键的唯一性也使数据存储更加集中。

#### 2.2 HBase 的使用

​        由于自身机器配置的限制，选择使用 BDKIT 来完成本次实验，因此报告将省略 HBase 的安装及配置操作。

​        在 bdkit.info 页面上选择 Hadoop 3.1.4, Spark 3.0.2, Hbase 2.2.6 镜像创建集群 zrq191098346-cluster，在 VS Code 的 Terminal 中输入 start-hbase.sh，完成 HBase 的启动。

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-18 205654.jpg)

#### 2.3 Java 编程实现 HBase Shell 操作

- 创建表

```java
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
```

- 删除表


```java
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
```

- 指定行/列中插入数据


```java
public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        //获得Table接口,需要传入表名
        Table table =connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("insert record " + rowKey + " to table " + tableName + " ok.");
}
```

- **在已有表中增加新的列族**

```Java
public static void insert2(String tableName, String family) throws IOException {  
        //先创建一个列族，才能给这个列族，添加属性
        HColumnDescriptor newFamliy = new HColumnDescriptor(family);
        admin.addColumn(TableName.valueOf(tableName), newFamliy);
        System.out.println("insert family " + family + " to table " + tableName + " ok.");
}
```

- 删除表中的指定行

```Java
public static void delete(String tableName, String rowKey) throws IOException {
        //获得Table接口,需要传入表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        //Delete delete = new Delete(Bytes.toBytes(rowKey));
        Delete delete = new Delete("course".getBytes());
        table.delete(delete);
        System.out.println("delete " + rowKey + " courses record sucessfully.");
}
```

- **单条件按查询，查询多条记录**

```Java
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
```

- **具体功能实现**

```Java
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

        //创建表course
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
        //先增加新的列族Contact
        insert2("student","Contact");
        //再向新的列族中插入新列
        insert("student","2015001","Contact","Email","lilie@qq.com");
        insert("student","2015002","Contact","Email","hmm@qq.com");
        insert("student","2015003","Contact","Email","zs@qq.com");

        //删除学号为2015003的学生的选课记录
        delete("student","2015003");

        //删除表
        dropTable("student");
        dropTable("course");
}
```



### 3 实验结果

#### 3.1 代码运行结果

- mvn package 打 jar 包

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 153950.jpg)

- hadoop jar target/hbasetest-1.0-SNAPSHOT.jar 运行 jar 包

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165253.jpg)

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165338.jpg)

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165653.jpg)

- 创建表 student 并插入数据

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165402.jpg)

- 创建表 course 并插入数据

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165426.jpg)

- 查询选 Computer Science 的学生成绩

<img src="C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165454.jpg" style="zoom:170%;" />

- 向表 student 中插入新的列族，列和数据

<img src="C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165539.jpg" style="zoom:150%;" />

​       通过查看 hbase 的端口，可以看到新列族 Contact 已经成功插入了

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 171730.jpg)

- 删除学生 2015003 的选课信息

<img src="C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165602.jpg" style="zoom:150%;" />

- 删除表

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 165630.jpg)

​       可以看到，所有表都已被成功删除

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 172516.jpg)



#### 3.2 使用 Shell 完成相同任务

- 启动 HBase Shell

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-18 205732.jpg)

- 创建表 student 并插入数据

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 172239.jpg)

- 创建表 course 并插入数据

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 172323.jpg)

- 查询选 Computer Science 的学生成绩

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 211645.jpg)

- 向表 student 中插入新的列族，列和数据

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 172344.jpg)

- 删除学生 2015003 的选课信息

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 172406.jpg)

- 删除表

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 172434.jpg)



### 4 问题总结及解决方案

- 初次运行 jar 包会出现下述报错

  原因：pom.xml 文件中 hbase 的配置与集群中镜像的版本不兼容，将配置改为 2.2.6/2.4.8 问题解决

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-17 231823.jpg)

- 报错：nonode for hbase 

​       原因：运行 jar 包前忘记启动 hbase

![](C:\Users\THINK\Desktop\QQ图片20211118200836.jpg)

- cannot find symbol：getScanner() 

​       原因：调用该方法的变量类型应该是 Table 而非 String，需要把传入的 String 类型变量转换为 Table 类型；

​       代码：Table table = connection.getTable(TableName.valueOf(tableName));

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 140716.jpg)

- 报错：reached end of file while parsing

  原因：缺少一个大括号，导致函数无法闭合

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 144450.jpg)

- 报错：cannot find symbol raw() , keyvalue

  原因：raw() 方法已被弃用，查询 API，得知新方法为 listCells()

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 140716.jpg)

​       然而修改后仍报错，发现是因为没有 import 需要的包，增加后编译通过：

​       import org.apache.hadoop.hbase.Cell;

​       import org.apache.hadoop.hbase.CellUtil;

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 152329.jpg)

- 报错：must be caught or declared to be thrown

  原因：函数未添加 throws IOException，添加后编译通过

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 153604.jpg)

- 报错：column family contact does not exist in region student

  原因：需要编写新的函数来增加列族，而不能直接使用 insert() 函数

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-19 160110.jpg)



### 5 其他思考

- 在实验的开始，我天真地以为使用 BDKIT 会比在自己的机器上安装 hbase 并运行容易更多，但经过几天的亲身体验，发现即使是使用现成的环境，也有很多问题需要注意。网站有时可能会不稳定，版本的兼容性，失去了 IDE 后调试不方便，maven 文件的编译运行方法，旧版本方法的废弃等，都有可能成为实验成功路上的绊脚石。无论使用哪一种方法，理解知识的内涵都是最关键的；

- 在使用 BDKIT 的过程中，我发现 java 程序不能直接运行，每次都要经过 mvn clean, mvn compile 才能找出编译中的错误，打包后运行有时还会出现错误，非常繁琐。而使用 IntellJ idea 却能够直接运行 java 文件，这是为什么呢？与助教老师讨论后，我明白这是 java 正常的运行方式，而 IDE 帮我们做了，因此看上去好像可以直接运行 java 文件。

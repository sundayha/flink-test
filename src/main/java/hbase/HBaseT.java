package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HBaseT {

    public static void main(String[] args) throws Exception {
        //List<String> strings = new ArrayList<>(3);
        //strings.add("花钱");
        //strings.add("操");
        //strings.add("逼");
        User user = new User();
        user.setRowKey("101");
        user.setAge("29");
        user.setSex("男");
        //user.setHobby(strings);
        user.setName("顶呱呱，跨啦啦");
        //createTable("user", "information");
        //createTable(user);
        //insertData("user", user);
        //deleteTable("user");

        //getAllData("user", User.class).forEach(
        //        s -> {
        //            System.out.println(s.getRowKey());
        //            System.out.println(s.getName());
        //            System.out.println(s.getSex());
        //            System.out.println(s.getAge());
        //        }
        //);

        getResult("user", "100", User.class).forEach(
                s -> {
                    System.out.println(s.getRowKey());
                    System.out.println(s.getName());
                    System.out.println(s.getSex());
                    System.out.println(s.getAge());
                }
        );

    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-04 10:52
     * @apiNote 初始化 hBase 配置
     * @return Configuration
     */
    public static Configuration initConfiguration() {

        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        configuration.set("hbase.zookeeper.quorum", "master");

        return configuration;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-04 10:55
     * @apiNote 返回一个 Connection 实例。调用者需要调用 Connection.close()
     * @return Connection
     */
    public static Connection initHBase() throws IOException {

        return ConnectionFactory.createConnection(initConfiguration());
    }
    
    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-04 10:56
     * @param nThreads 线程池中的线程数
     * @apiNote 返回一个 Connection 实例。指定线程池，执行批量操作。调用者需要调用 Connection.close()
     * @return Connection
     */
    public static Connection initBatchHBase(int nThreads) throws IOException {

        ExecutorService executor = Executors.newFixedThreadPool(nThreads);

        return ConnectionFactory.createConnection(initConfiguration(), executor);
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-04 11:53
     * @param tableName 表名
     * @param columnFamilyNames 列簇名
     * @apiNote 给出表名与列簇名生成表
     */
    public static void createTable(String tableName, String ... columnFamilyNames) throws IOException {

        try (Connection connection = initHBase(); Admin admin = connection.getAdmin()) {

            createTable(admin, tableName, columnFamilyNames);
        }
    }


    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 10:44
     * @param obj 表实体
     * @apiNote 根据实体上的注解来生成表
     */
    public static <T> void createTable(T obj) throws Exception {

        try (Connection connection = initHBase(); Admin admin = connection.getAdmin()) {

            Class<?> aClass = obj.getClass();

            if (aClass.isAnnotationPresent(HBaseModel.class)) {

                HBaseModel hBaseModelAnnotation = aClass.getAnnotation(HBaseModel.class);
                // 得到表名
                String tableName = hBaseModelAnnotation.tableName();

                Field[] fields = aClass.getDeclaredFields();

                Set<String> strings = new HashSet<>();

                for (Field field : fields) {

                    if (field.isAnnotationPresent(HBaseModelProperty.class)) {

                        HBaseModelProperty hBaseModelProperty = field.getAnnotation(HBaseModelProperty.class);
                        // 得到列簇名，并去重
                        strings.add(hBaseModelProperty.family());
                    }
                }

                createTable(admin, tableName, strings.toArray(new String[0]));
            }
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 10:45
     * @param admin admin 实例
     * @param tableName 表名
     * @param columnFamilyNames 列簇名
     * @apiNote 创建表
     */
    private static void createTable(Admin admin, String tableName, String[] columnFamilyNames) throws IOException {

        TableName newTableName = TableName.valueOf(tableName);

        if (admin.tableExists(newTableName)) {

            System.out.println("表存在");
        } else {
            // 如果表的区域不可用，那么添加 columnFamilyName 并创建表
            if (!admin.isTableAvailable(newTableName)) {
                HTableDescriptor descriptor = new HTableDescriptor(newTableName);
                for (String columnFamilyName : columnFamilyNames) {
                    descriptor.addFamily(new HColumnDescriptor(columnFamilyName));
                }
                // hBase 中，表相当于 mysql 的 db。columnFamily 相当于 mysql 中的表
                admin.createTable(descriptor);
            }
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 11:07
     * @param tableName 要插入数据的表名
     * @param obj 插入的
     * @apiNote 向表插入数据
     */
    public static <T> void insertData(String tableName, T obj) throws Exception {

        try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(tableName))) {

            table.put(modelToPut(obj));
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 11:14
     * @param obj 要转成 put 的对象
     * @apiNote 将实体转成 put 对象
     * @return Put 对象
     */
    public static <T> Put modelToPut(T obj) throws Exception {

        Class<?> aClass = obj.getClass();

        Field[] fields = aClass.getDeclaredFields();

        Put put = new Put(Bytes.toBytes(modelToRowKey(obj)));

        for (Field field : fields) {

            Field fiele1 = aClass.getDeclaredField(field.getName());

            fiele1.setAccessible(true);
            // 类成员上是否含有该注解
            if (!field.isAnnotationPresent(HBaseModelProperty.class)) {
                continue;
            }
            // 从类成员上得到该注解
            HBaseModelProperty hBaseModelPropertyAnnotation = field.getAnnotation(HBaseModelProperty.class);
            // 如果该注解中含有 rowKey 则继续下一个
            if ("rowKey".equals(hBaseModelPropertyAnnotation.family()) || "rowKey".equals(hBaseModelPropertyAnnotation.qualifier())) {
                continue;
            }

            put.addColumn(Bytes.toBytes(hBaseModelPropertyAnnotation.family()), Bytes.toBytes(hBaseModelPropertyAnnotation.qualifier()), Bytes.toBytes(fiele1.get(obj).toString()));

            System.out.println("family: " + hBaseModelPropertyAnnotation.family() + " qualifier: " + hBaseModelPropertyAnnotation.qualifier() + " value: " + fiele1.get(obj));
        }

        return put;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 11:19
     * @param obj 实体对象
     * @apiNote 取得 rowKey 的值，实体对象中必须有一个成员为 rowKey
     * @return String
     */
    public static <T> String modelToRowKey(T obj) throws Exception {

        Class<?> aClass = obj.getClass();

        Field field = aClass.getDeclaredField("rowKey");

        field.setAccessible(true);

        return field.get(obj).toString();
    }

    public static <T> List<T> getAllData(String tableName, Class<T> aClass) throws Exception {

        List<T> ts = new ArrayList<>();

        Table table = initHBase().getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        // 扫描全表
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {

            if (!result.isEmpty()) {

                // 取出每一行的数据
                ts.add(resultToModel(result, aClass));
            }
        }

        return ts;
    }

    public static <T> List<T> getResult(String tableName, String rowKey, Class<T> aClass) throws Exception {

        List<T> ts = new ArrayList<>();

        try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(tableName))) {

            Get get = new Get(rowKey.getBytes());

            if (!get.isCheckExistenceOnly()) {

                Result result = table.get(get);

                if (!result.isEmpty()) {

                    ts.add(resultToModel(result, aClass));
                }
            }
        }

        return ts;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 16:07
     * @param result hBase 取出来的结果
     * @param modelClass 要转换的 model 类
     * @apiNote 把 hBase 取出来的结果，转换成需要转换的 model 类
     * @return T 返回转换好的 model
     */
    public static <T> T resultToModel(Result result, Class<T> modelClass) throws Exception {

        // 初始化 model
        T t = modelClass.newInstance();

        // 获得类成员
        Field[] fields = modelClass.getDeclaredFields();

        for (Field field : fields) {

            // 获得 private 成员访问权限
            field.setAccessible(true);

            HBaseModelProperty hBaseModelProperty = field.getAnnotation(HBaseModelProperty.class);

            // 拼接 set 方法名
            String fieldSetMethodName = getSetMethod(field.getName());

            // 声明方法
            Method method = modelClass.getDeclaredMethod(fieldSetMethodName, field.getType());

            // 调用 set 方法给初始化好的对象成员赋值
            if ("rowKey".equals(field.getName())) {

                method.invoke(t, Bytes.toString(result.getRow()));
            } else {

                method.invoke(t, Bytes.toString(result.getValue(Bytes.toBytes(hBaseModelProperty.family()), Bytes.toBytes(hBaseModelProperty.qualifier()))));
            }
        }

        return t;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 16:42
     * @param word 需要拼接方法名的成员名称
     * @apiNote 拼接 set 方法名
     * @return String
     */
    private static String getSetMethod(String word) {

        char[] wordChar = word.toCharArray();

        wordChar[0] -= 32;

        return "set".concat(String.valueOf(wordChar));
    }

    public static void deleteTable(String tableName) throws IOException {

        TableName tablename = TableName.valueOf(tableName);

        Admin admin = initHBase().getAdmin();

        admin.disableTable(tablename);

        admin.deleteTable(tablename);
    }
}

package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HBaseMapper {

    public static void main(String[] args) throws Exception {
        User user = new User();
        user.setRowKey("101");
        user.setAge("29");
        user.setSex("男");
        user.setName("顶呱呱，跨啦啦");

        //createTable("user", "information");
        //createTable(user);
        //insert("user", user);

        //insertSelective

        //deleteTable("use111r");

        //getAllData("user", User.class).forEach(
        //        s -> {
        //            System.out.println(s.getRowKey());
        //            System.out.println(s.getName());
        //            System.out.println(s.getSex());
        //            System.out.println(s.getAge());
        //            //System.out.println(s.toString());
        //            System.out.println("==========");
        //        }
        //);

        //getResultByRowKey("user", "100", User.class).forEach(
        //        s -> {
        //            System.out.println(s.getRowKey());
        //            System.out.println(s.getName());
        //            System.out.println(s.getSex());
        //            System.out.println(s.getAge());
        //        }
        //);



        deleteByRowKey("user", "104");

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
    public static void createTable(String tableName, String ... columnFamilyNames) {

        try (Connection connection = initHBase(); Admin admin = connection.getAdmin()) {

            HBaseUtil.createTable(admin, tableName, columnFamilyNames);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 10:44
     * @param obj 表实体
     * @apiNote 根据实体上的注解来生成表
     */
    public static <T> void createTable(T obj) {

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

                HBaseUtil.createTable(admin, tableName, strings.toArray(new String[0]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 11:07
     * @param tableName 表名
     * @param obj 要插入 hBase 的实体对象
     * @apiNote 向表插入数据
     */
    public static <T> void insert(String tableName, T obj) {

        try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(tableName))) {

            table.put(HBaseUtil.modelToPut(obj));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static <T> void insertSelective(String tableName, T obj) throws Exception {

    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-08 09:52
     * @param tableName 表名
     * @param aClass 从 hBase 转换出来的实体对象
     * @apiNote 获取表的全部数据。并转换成 List<T>。尽量少使用该方法。因为表的数据量巨大。
     * @return List<T>
     */
    public static <T> List<T> getAllData(String tableName, Class<T> aClass) {

        List<T> results = new ArrayList<>();

        try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(tableName))) {

            Scan scan = new Scan();
            // 扫描全表
            ResultScanner resultScanner = table.getScanner(scan);

            for (Result result : resultScanner) {

                if (!result.isEmpty()) {
                    // 取出每一行的数据
                    results.add(HBaseUtil.resultToModel(result, aClass));
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
        }

        return results;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-08 10:12
     * @param tableName 表名
     * @param rowKey 行主键
     * @param aClass 从 hBase 转换出来的实体对象
     * @apiNote 根据主键得到数据
     * @return List<T>
     */
    public static <T> List<T> getListByRowKey(String tableName, String rowKey, Class<T> aClass) {

        List<T> results = new ArrayList<>();

        try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(tableName))) {

            Get get = new Get(rowKey.getBytes());

            if (!get.isCheckExistenceOnly()) {

                Result result = table.get(get);

                if (!result.isEmpty()) {

                    results.add(HBaseUtil.resultToModel(result, aClass));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    public static void deleteByRowKey(String tableName, String rowKey) {

        try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(tableName))) {

            Delete delete = new Delete(Bytes.toBytes(rowKey));

            table.delete(delete);
            //tTable.delete(delete);
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-08 10:18
     * @param tableName 表名
     * @apiNote 删除表
     */
    public static void deleteTable(String tableName) {

        TableName tablename = TableName.valueOf(tableName);

        try (Connection connection = initHBase(); Admin admin = connection.getAdmin()) {

            if (admin.tableExists(tablename)) {
                // 先禁用表
                admin.disableTable(tablename);
                // 再删除表
                admin.deleteTable(tablename);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

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
     * @param aClass 表实体 class
     * @apiNote 根据实体上的注解来生成表
     */
    public static <T> void createTable(Class<T> aClass) {

        try (Connection connection = initHBase(); Admin admin = connection.getAdmin()) {

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
     * 时间：2019-04-10 13:47
     * @param aClass 实体 class
     * @apiNote 覆盖表。先删除，再创建
     */
    public static <T> void createTableOverwrite(Class<T> aClass) {
        try {
            deleteTable(aClass);
            createTable(aClass);
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

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-10 13:29
     * @param obj 要插入 hBase 的实体对象
     * @apiNote 不需要指定表名，直接从注解中拿到表名，传递过来的类中必须带有 HBaseModel 注解的 tableName
     */
    public static <T> void insert(T obj) {

        Class<?> aClass = obj.getClass();

        if (aClass.isAnnotationPresent(HBaseModel.class)) {

            HBaseModel hBaseModel = aClass.getAnnotation(HBaseModel.class);

            String tableName = hBaseModel.tableName();

            try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(tableName))) {

                table.put(HBaseUtil.modelToPut(obj));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-08 09:52
     * @param aClass 从 hBase 转换出来的实体 class
     * @apiNote 获取表的全部数据。并转换成 List<T>。尽量少使用该方法。因为表的数据量巨大。
     * @return List<T>
     */
    public static <T> List<T> getAllData(Class<T> aClass) {

        List<T> results = new ArrayList<>();

        if (aClass.isAnnotationPresent(HBaseModel.class)) {

            HBaseModel hBaseModel = aClass.getAnnotation(HBaseModel.class);

            try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(hBaseModel.tableName()))) {

                Scan scan = new Scan();
                // 扫描全表
                ResultScanner resultScanner = table.getScanner(scan);

                for (Result result : resultScanner) {

                    if (!result.isEmpty()) {
                        // 取出每一行的数据，添加到 list 中
                        results.add(HBaseUtil.resultToModel(result, aClass));
                    }
                }
            } catch (Exception e) {

                e.printStackTrace();
            }
        }

        return results;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-08 10:12
     * @param rowKey 行主键
     * @param aClass 从 hBase 转换出来的实体 class
     * @apiNote 根据主键得到 list 数据
     * @return List<T>
     */
    public static <T> List<T> getListByRowKey(String rowKey, Class<T> aClass) {

        List<T> results = new ArrayList<>();

        if (aClass.isAnnotationPresent(HBaseModel.class)) {

            HBaseModel hBaseModel = aClass.getAnnotation(HBaseModel.class);

            try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(hBaseModel.tableName()))) {

                Get get = new Get(Bytes.toBytes(rowKey));

                if (!get.isCheckExistenceOnly()) {

                    Result result = table.get(get);

                    if (!result.isEmpty()) {

                        results.add(HBaseUtil.resultToModel(result, aClass));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return results;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-22 09:39
     * @param rowKey 行主键
     * @param aClass 从 hBase 转换出来的实体 class
     * @apiNote 根据主键得到实体数据
     * @return T
     */
    public static <T> T getObjectByRowKey(String rowKey, Class<T> aClass) {

        T t = null;

        if (aClass.isAnnotationPresent(HBaseModel.class)) {

            HBaseModel hBaseModel = aClass.getAnnotation(HBaseModel.class);

            try (Connection connection = initHBase(); Table table = connection.getTable(TableName.valueOf(hBaseModel.tableName()))) {

                Get get = new Get(Bytes.toBytes(rowKey));

                if (!get.isCheckExistenceOnly()) {

                    Result result = table.get(get);

                    if (!result.isEmpty()) {

                        t = HBaseUtil.resultToModel(result, aClass);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return t;
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-22 09:48
     * @param tableName 表名
     * @param rowKey 表主键
     * @apiNote 根据 rowKey 删除行
     */
    public static void deleteRowByRowKey(String tableName, String rowKey) {

        deleteRowByRowKey(TableName.valueOf(tableName), rowKey);
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-22 09:51
     * @param aClass 从 hBase 中要删除行的实体 class
     * @param rowKey 表主键
     * @apiNote 根据 rowKey 删除行
     */
    public static <T> void deleteRowByRowKey(Class<T> aClass, String rowKey) {

        HBaseModel hBaseModel = aClass.getDeclaredAnnotation(HBaseModel.class);

        if (aClass.isAnnotationPresent(HBaseModel.class)) {

            deleteRowByRowKey(TableName.valueOf(hBaseModel.tableName()), rowKey);
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-22 09:53
     * @param tableName TableName 对象
     * @param rowKey 表主键
     * @apiNote 合并删除行的公共方法
     */
    private static void deleteRowByRowKey(TableName tableName, String rowKey) {

        try (Connection connection = initHBase(); Table table = connection.getTable(tableName)) {

            Delete delete = new Delete(Bytes.toBytes(rowKey));

            table.delete(delete);
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

        deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-10 13:40
     * @param aClass 要删除表的实体 class
     * @apiNote 在 hBase 中删除该表
     */
    public static <T> void deleteTable(Class<T> aClass) {

        HBaseModel hBaseModel = aClass.getDeclaredAnnotation(HBaseModel.class);

        if (aClass.isAnnotationPresent(HBaseModel.class)) {

            deleteTable(TableName.valueOf(hBaseModel.tableName()));
        }
    }

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-10 13:41
     * @param tableName TableName 的对象
     * @apiNote 为删除表提供的公共方法
     */
    private static void deleteTable(TableName tableName) {

        try (Connection connection = initHBase(); Admin admin = connection.getAdmin()) {

            if (admin.tableExists(tableName)) {
                // 先禁用表
                admin.disableTable(tableName);
                // 再删除表
                admin.deleteTable(tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

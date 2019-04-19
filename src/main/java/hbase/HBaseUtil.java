package hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HBaseUtil {

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 10:45
     * @param admin admin 实例
     * @param tableName 表名
     * @param columnFamilyNames 列簇名
     * @apiNote 创建表
     */
    public static void createTable(Admin admin, String tableName, String[] columnFamilyNames) throws Exception {

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
            // 如果该类成员上不含有此注解，那么走下一个 field
            if (!field.isAnnotationPresent(HBaseModelProperty.class)) {
                continue;
            }
            // 从类成员上得到该注解
            HBaseModelProperty hBaseModelPropertyAnnotation = field.getAnnotation(HBaseModelProperty.class);
            // 如果该注解中含有 rowKey 则继续下一个 field。rowKey 不包含在列簇中
            if ("rowKey".equals(hBaseModelPropertyAnnotation.family()) || "rowKey".equals(hBaseModelPropertyAnnotation.qualifier())) {
                continue;
            }

            if (fiele1.get(obj) == null) {
                continue;
            }

            // 如果成员属性为 byte[]，可能代表为文件的字节数组
            if (fiele1.getType().getTypeName().equals("byte[]")) {

                put.addColumn(Bytes.toBytes(hBaseModelPropertyAnnotation.family()), Bytes.toBytes(hBaseModelPropertyAnnotation.qualifier()), (byte[])fiele1.get(obj));
            } else {

                put.addColumn(Bytes.toBytes(hBaseModelPropertyAnnotation.family()), Bytes.toBytes(hBaseModelPropertyAnnotation.qualifier()), Bytes.toBytes(fiele1.get(obj).toString()));
            }

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

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019-04-05 16:07
     * @param result hBase 取出来的结果
     * @param modelClass 要转换的 model 类
     * @apiNote 把 hBase 取出来的结果，转换成需要转换的 model 对象
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

                // 如果成员属性为 byte[]，可能代表为文件的字节数组
                if (field.getType().getTypeName().equals("byte[]")) {

                    method.invoke(t, (Object) result.getValue(Bytes.toBytes(hBaseModelProperty.family()), Bytes.toBytes(hBaseModelProperty.qualifier())));
                } else {

                    method.invoke(t, Bytes.toString(result.getValue(Bytes.toBytes(hBaseModelProperty.family()), Bytes.toBytes(hBaseModelProperty.qualifier()))));
                }
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
}

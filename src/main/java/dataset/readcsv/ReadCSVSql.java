package dataset.readcsv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.List;

/**
 * @author 张博【zhangb@lianliantech.cn】
 *
 * 加载 csv 文件并使用 sql 查询结果
 */
public class ReadCSVSql {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment see = ExecutionEnvironment.getExecutionEnvironment();
        // 创建表环境
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(see);
        // 定义 pojo 的类型
        PojoTypeInfo<WaferHead> pojoType = (PojoTypeInfo<WaferHead>) TypeExtractor.createTypeInfo(WaferHead.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"id", "waferId", "path", "state"};
        // 创建 PojoCsvInputFormat，读取 csv
        PojoCsvInputFormat<WaferHead> csvInput = new PojoCsvInputFormat<>(new Path("/Users/zhangbo/Downloads/ND_SVD_DEV_wafer_aoi_head.csv"), pojoType, fieldOrder);
        // 创建 dataSet
        DataSet<WaferHead> dataSet = see.createInput(csvInput, pojoType);
        // 把 dataSet 转成 table
        Table table = tableEnvironment.fromDataSet(dataSet);
        // 注册该 table
        tableEnvironment.registerTable("waferHead", table);
        // 查询该 table
        Table sql = tableEnvironment.sqlQuery("select * from waferHead where id in (1, 2)");
        // 把查询结果转成 dataSet
        DataSet<WaferHead> result = tableEnvironment.toDataSet(sql, WaferHead.class);
        //result.map(new MapFunction<WaferHead, Tuple4<String, String, String, String>>() {
        //
        //    @Override
        //    public Tuple4<String, String, String, String> map(WaferHead value) throws Exception {
        //        System.out.println(value.getId());
        //        System.out.println(value.getWaferId());
        //        return new Tuple4<>(value.getId(), value.getWaferId(), value.getPath(), value.getState());
        //    }
        //});
        List<WaferHead> list =  result.collect();
        list.forEach(s -> {
            System.out.println(s.getId());
            System.out.println(s.getWaferId());
        });
        //see.execute();
    }
}

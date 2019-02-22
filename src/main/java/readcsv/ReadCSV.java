package readcsv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class ReadCSV {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment see = ExecutionEnvironment.getExecutionEnvironment();
        //DataSet<WaferHead> dataSet = see.readCsvFile("/Users/zhangbo/Downloads/ND_SVD_DEV_wafer_aoi_head.csv").pojoType(WaferHead.class);
        PojoTypeInfo<WaferHead> pojoType = (PojoTypeInfo<WaferHead>) TypeExtractor.createTypeInfo(WaferHead.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"id", "waferId", "path", "state"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<WaferHead> csvInput = new PojoCsvInputFormat<>(new Path("/Users/zhangbo/Downloads/ND_SVD_DEV_wafer_aoi_head.csv"), pojoType, fieldOrder);

        DataSet<WaferHead> dataSet = see.createInput(csvInput, pojoType);


        dataSet.print();
        see.execute();
    }
}

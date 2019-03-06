package dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.StringValue;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class DataSourceTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(2);

        DataSet<StringValue> stringValueDataSet = environment.readTextFileWithValue("/Users/zhangbo/Desktop/flink_loggers/flink_test/info/flink_test_info_logger.2019-03-04.0.log");

        stringValueDataSet.first(10).print();
    }
}

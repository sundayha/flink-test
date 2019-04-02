package hadoop.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.TimeZone;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HadoopTableT {

    public static void main(String[] args) {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        TableConfig tableConfig = new TableConfig();
        tableConfig.setTimeZone(TimeZone.getDefault());

        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment, tableConfig);

        //tableEnvironment.registerTableSink("images", );
        BucketingSink<Long> bk = new BucketingSink<>("hdfs://localhost:9000/home/zb/zhangbo/zhangbo-10000.txt");
        //tableEnvironment.registerTableSink("images", bk);
    }
}

package hadoop;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
//import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.core.fs.FileSystem;
//import org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction;
//import org.apache.flink.util.Collector;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author 张博
 */
public class HadoopT {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataSet<Long> dataSet = executionEnvironment.generateSequence(0, 10000);
        //dataSet.writeAsText("file:///Users/zhangbo/Downloads/flinkOut/zhangbo-1000.txt", FileSystem.WriteMode.OVERWRITE);
        dataSet.writeAsText("hdfs://master:9000/home/zb/zhangboj-1000.txt", FileSystem.WriteMode.OVERWRITE);

        executionEnvironment.execute();
    }
}

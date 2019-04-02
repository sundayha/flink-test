package hadoop;

//import org.apache.flink.api.common.functions.FlatMapFunction;

//import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
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
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HadoopStreamT {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStream<Long> ds = executionEnvironment.generateSequence(0, 100);

        BucketingSink<Long> bk = new BucketingSink<>("hdfs://master:9000/home/zb/zhangbo/zhangbo-100.txt");

        Configuration configuration = new Configuration();
        configuration.setString("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        configuration.setString("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

        //BucketingSink.createHadoopFileSystem(new Path("hdfs://master:9000/home/zb/zhangbo"), configuration);

        bk.setInactiveBucketCheckInterval(1L);
        //bk.set(1L);
        ds.addSink(bk);



        executionEnvironment.execute();
    }
}

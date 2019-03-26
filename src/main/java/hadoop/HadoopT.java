package hadoop;

//import org.apache.flink.api.common.functions.FlatMapFunction;

//import org.apache.flink.api.common.functions.FlatMapFunction;

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
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HadoopT {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataSet<Long> dataSet = executionEnvironment.generateSequence(0, 100000);
        dataSet.writeAsText("file:///Users/zhangbo/Downloads/flinkOut/zhangbo.txt", FileSystem.WriteMode.OVERWRITE);
        //dataSet.writeAsText("hdfs://localhost:9000/home/zb/zhangboj10000.txt", FileSystem.WriteMode.OVERWRITE);

       // DataSet<Tuple2<LongWritable, Text>> text = dataSet.flatMap(new FlatMapFunction<Long, Tuple2<LongWritable, Text>>() {
       //     @Override
       //     public void flatMap(Long value, Collector<Tuple2<LongWritable, Text>> out) throws Exception {
       //         out.collect(new Tuple2<>(new LongWritable(value), new Text("哈哈")));
       //     }
       // });
       //
       // DataSet<Tuple2<Text, LongWritable>> result = text.flatMap(new HadoopMapFunction<LongWritable, Text, Text, LongWritable>(
       //        new Tokenizer()
       //));
       // //.groupBy(0).reduceGroup(new HadoopReduceCombineFunction<Text, LongWritable, Text, LongWritable>(
       // //        new Count(), new Count()
       // //))
       //
       // Job job = Job.getInstance();
       // HadoopOutputFormat<Text, LongWritable> hadoopOF = new HadoopOutputFormat<>(new TextOutputFormat<>(), job);
       // TextOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/home/zb/zhangbo"));
       // hadoopOF.getConfiguration().set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
       // hadoopOF.getConfiguration().set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
       // hadoopOF.getConfiguration().set("dfs.datanode.max.transfer.threads","8192");
       // hadoopOF.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize","16777216");
       // hadoopOF.getConfiguration().set("dfs.replication","1");
       // //hadoopOF.getConfiguration().set("dfs.qjm.operations.timeout","100000000s");
       // result.print();
       // result.output(hadoopOF);
        executionEnvironment.execute();
    }
}

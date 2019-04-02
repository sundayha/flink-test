package hadoop.compatibility;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.io.PrimitiveInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * @author 张博
 *
 * 使用 flink 结合 hadoop api 把数据放入 hdfs 中
 */
public class HadoopT {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        uploadFile(executionEnvironment);

        executionEnvironment.execute();
    }

    private static void outFile(ExecutionEnvironment executionEnvironment) throws IOException {
        DataSet<Long> dataSet = executionEnvironment.generateSequence(0, 10000);

        DataSet<Tuple2<LongWritable, Text>> text = dataSet.flatMap(new FlatMapFunction<Long, Tuple2<LongWritable, Text>>() {
            @Override
            public void flatMap(Long value, Collector<Tuple2<LongWritable, Text>> out) throws Exception {
                out.collect(new Tuple2<>(new LongWritable(value), new Text("")));
            }
        });

        DataSet<Tuple2<Text, LongWritable>> result = text.flatMap(new HadoopMapFunction<LongWritable, Text, Text, LongWritable>(
                new Tokenizer()
        ));
        //.groupBy(0).reduceGroup(new HadoopReduceCombineFunction<Text, LongWritable, Text, LongWritable>(
        //        new Count(), new Count()
        //))

        Job job = Job.getInstance();
        HadoopOutputFormat<Text, LongWritable> hadoopOF = new HadoopOutputFormat<>(new TextOutputFormat<>(), job);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/home/zb/zhangbo"));
        hadoopOF.getConfiguration().set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        hadoopOF.getConfiguration().set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
        hadoopOF.getConfiguration().set("dfs.datanode.max.transfer.threads","8192");
        result.output(hadoopOF);
    }

    private static void uploadFile(ExecutionEnvironment executionEnvironment) throws Exception {
        TypeInformation<Byte> typeInfo = TypeExtractor.createTypeInfo(Byte.class);

        PrimitiveInputFormat<Byte> primitiveInputFormat = new PrimitiveInputFormat<>(new org.apache.flink.core.fs.Path("/Users/zhangbo/Downloads/flinkOut//SI10953-14_X_77_Y_55.jpg"), Byte.class);

        DataSet<Byte> dataSet = executionEnvironment.createInput(primitiveInputFormat, typeInfo);

        dataSet.print();
    }
}

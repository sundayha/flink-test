package hadoop;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author 张博
 */
public class HadoopT {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataSet<Long> dataSet = executionEnvironment.generateSequence(0, 10000);
        //dataSet.writeAsText("hdfs://master:9000/home/zb/zhangboj-1000.txt", FileSystem.WriteMode.OVERWRITE);

        readAsTest(executionEnvironment);

        //executionEnvironment.execute();
    }

    private static void writeAsText(DataSet<String> dataSet) {
        //dataSet.w.writeAsText("hdfs://master:9000/home/zhangboj-10000.txt", FileSystem.WriteMode.OVERWRITE);
    }

    private static void readAsTest(ExecutionEnvironment executionEnvironment) throws Exception {
        DataSet<String> dataSet = executionEnvironment.readTextFile("hdfs://master:9000/home/zb/zhangboj-10000.txt");
        //DataSet<ByteValue> dataSet = executionEnvironment.readFile(new SerializedInputFormat<>(), "hdfs://master:9000/user/zhangbo/SI10953-14_X_77_Y_55.jpg");

        //dataSet.writeAsText("file:///Users/zhangbo/Downloads/flinkOut//SI10953-14_X_77_Y_55.jpg");
        //dataSet.write(new SerializedOutputFormat<>(), "file:///Users/zhangbo/Downloads/flinkOut//SI10953-14_X_77_Y_55.jpg");
        //dataSet.print();

        //HadoopInputFormat inputFormat = HadoopInputs.readHadoopFile();
    }
}

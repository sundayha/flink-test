package dataset;

import dataset.readcsv.Log;
import dataset.readcsv.WaferHead;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class DataSourceTest {

    public static void main(String[] args) throws Exception {

        String logFilePath = "/Users/zhangbo/Desktop/flink_loggers/flink_test/info/flink_test_info_logger.2019-03-06.0.log";
        String logDirectoryPath = "/Users/zhangbo/Desktop/flink_loggers/flink_test/warn";
        String csvFilePath = "/Users/zhangbo/Downloads/ND_SVD_DEV_wafer_aoi_head.csv";
        String writeFilePath = "/Users/zhangbo/Desktop/flink_loggers/flink_test/xxx.csv";

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(3);

        DataSet<String> ds = environment.readTextFile(logFilePath);

        DataSet<StringValue> ds1 = environment.readTextFileWithValue(logFilePath);

        DataSource<WaferHead> ds2 = environment.readCsvFile(csvFilePath).pojoType(WaferHead.class, "id", "waferId", "path", "state");

        //readCsvFileT(ds2);

        //createInputT(environment, logFilePath);

        //readAllFile(environment, logDirectoryPath);

        writeAsCsvT(ds2, writeFilePath);

        environment.execute("write to csv");
    }

    public static void readTextFileT(DataSet<String> ds) throws Exception {
        ds.first(10).print();
    }

    public static void readTextFileWithValue(DataSet<StringValue> ds1) throws Exception {
        ds1.first(10).print();
    }

    public static void readCsvFileT(DataSource<WaferHead> ds2) throws Exception {

        DataSet<WaferHead> waferHeads =  ds2.rebalance().reduceGroup(new GroupReduceFunction<WaferHead, WaferHead>() {
            @Override
            public void reduce(Iterable<WaferHead> values, Collector<WaferHead> out) throws Exception {
                List<WaferHead> waferHeads1 = new ArrayList<>();
                values.forEach(waferHeads1::add);
                Collections.sort(waferHeads1);
                waferHeads1.forEach(out::collect);

            }
        });

        List<WaferHead> waferHeads1 = waferHeads.collect();
        waferHeads1.forEach(s -> {
            System.out.println(s.getId() + " " + s.getWaferId() + " " + s.getPath());
        });
    }

    /**
     * 使用 createInput 读取并解析文件，再转换成对象
     * @param environment
     * @param logFilePath
     * @throws Exception
     */
    public static void createInputT(ExecutionEnvironment environment, String logFilePath) throws Exception {

        String[] fieldOrder = new String[]{"dateTime", "logLevel", "threadName", "className", "msg"};

        PojoTypeInfo<Log> pojoTypeInfo = (PojoTypeInfo<Log>) TypeExtractor.createTypeInfo(Log.class);

        PojoCsvInputFormat<Log> inputFormat = new PojoCsvInputFormat<>(new Path(logFilePath), "\n", "  ", pojoTypeInfo, fieldOrder);

        DataSet<Log> logDataSet = environment.createInput(inputFormat, pojoTypeInfo);

        logDataSet.collect().forEach(s -> {
            System.out.println(s.getDateTime() + "---" + s.getLogLevel() + "---" + s.getThreadName() + "---" + s.getClassName() + "---" + s.getMsg());
        });
    }

    /**
     * 递归文件夹中的所有文件
     * @param environment
     * @param logDirectoryPath
     * @throws Exception
     */
    public static void readAllFile(ExecutionEnvironment environment, String logDirectoryPath) throws Exception {

        Configuration configuration = new Configuration();

        configuration.setBoolean("recursive.file.enumeration", true);

        DataSet<String> dataSet = environment.readTextFile(logDirectoryPath).withParameters(configuration);

        System.out.println(dataSet.collect().size());

    }

    /**
     * flink 暂时不支持全局排序，利用 partitionCustom 把 key 都落在一个区上，然后再根据这一个区，使用 sortPartition 将其排序，可以达到全局排序的效果。
     * @param ds2
     * @param writeFilePath
     * @throws Exception
     */
    public static void writeAsCsvT(DataSource<WaferHead> ds2, String writeFilePath) throws Exception {
        DataSet<Tuple4<Integer, String, String, String>> tuple4DataSet = ds2.flatMap(new FlatMapFunction<WaferHead, Tuple4<Integer, String, String, String>>() {
            @Override
            public void flatMap(WaferHead value, Collector<Tuple4<Integer, String, String, String>> out) throws Exception {
                out.collect(new Tuple4<>(value.getId(), value.getWaferId(), value.getPath(), value.getState()));
            }
        });

        DataSet<Tuple4<Integer, String, String, String>> tuple4DataSet1 = tuple4DataSet.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                System.out.println("key: " + key);
                System.out.println("numPartitions: " + numPartitions);
                System.out.println("key % numPartitions: " + key % numPartitions);
                return 0;
            }
        }, 0).sortPartition(0, Order.DESCENDING);
        //tuple4DataSet1.print();
        tuple4DataSet1.writeAsCsv(writeFilePath, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


    }
}

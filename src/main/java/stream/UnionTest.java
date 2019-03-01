package stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class UnionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> stream = environment.socketTextStream("localhost", 9000);
        DataStream<String> stream1 = environment.socketTextStream("localhost", 9001);
        DataStream<String> stream2 = environment.socketTextStream("localhost", 9002);

        DataStream<String> windowDataStream = stream.union(stream1, stream2).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                return System.currentTimeMillis();
            }
        });

        DataStream<Tuple2<String, Long>> tuple2DataStream = windowDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(new Tuple2<>("累加和：", Long.parseLong(value)));
            }
        });

        // 以5秒为一个窗口累加，但只有在5秒内有3个元素才会触发。下一次输入的会 fire 掉上次的窗口并打印出累加和
        tuple2DataStream.keyBy(0).timeWindow(Time.seconds(5)).trigger(CountTrigger.of(3)).sum(1).print();

        environment.execute();
    }
}

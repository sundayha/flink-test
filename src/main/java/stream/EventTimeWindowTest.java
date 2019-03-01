package stream;

import org.apache.flink.api.common.typeinfo.Types;
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
public class EventTimeWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置流时间特性为，事件时间
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream = environment.socketTextStream("localhost", 9000);

        // 分配提取递增时间的时间戳提取器
        DataStream<String> dataStreamTime = dataStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String element) {
                        return System.currentTimeMillis();
                    }
                });

        // 转换输入的数据格式
        DataStream<Tuple2<String, Integer>> tuple2DataStream = dataStreamTime
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> out.collect(new Tuple2<>("累加和：", Integer.parseInt(value))))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 按元组的第一位分组，按步长为5秒，窗口时间为10秒（每过五秒统计一次过去10秒内的累加和。该操可能会出现窗口重叠），计算取第二位累加
        tuple2DataStream.keyBy(0).timeWindow(Time.seconds(10), Time.seconds(5)).sum(1).print();

        // 以5秒为一个窗口累加，但只有在5秒内有3个元素才会触发。下一次输入的会 fire 掉上次的窗口并打印出累加和
        tuple2DataStream.keyBy(0).timeWindow(Time.seconds(5)).trigger(CountTrigger.of(3)).sum(1).print();

        // 自定义窗口函数，实现累加
        //tuple2DataStream.keyBy(0).timeWindow(Time.seconds(10), Time.seconds(5)).apply(new WindowFunction<Tuple2<String, Integer>, Object, Tuple, TimeWindow>() {
        //    @Override
        //    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Object> out) throws Exception {
        //        int sum = 0;
        //        for (Tuple2<String, Integer> tuple2 : input) {
        //            sum += tuple2.f1;
        //        }
        //        out.collect(sum);
        //    }
        //}).print();

        environment.execute();
    }
}

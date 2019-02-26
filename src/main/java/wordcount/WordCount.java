package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 张博【zhangb@lianliantech.cn】
 *
 * 控制台输入 nc -lk 9000。然后启动程序
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = see.socketTextStream("localhost", 9000);

        DataStream<Tuple2<String, Integer>> wordCount = text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            System.out.println("输入字符串：" + value);
            for (String word : value.split(" ")) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        wordCount.print();
        see.execute();
    }
}

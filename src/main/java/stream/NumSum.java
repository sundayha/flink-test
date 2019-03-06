package stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class NumSum {
    public static void main(String[] args) throws Exception {

        // 得到流处理环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从 socket 中读取数据
        DataStream<String> streamSource = see.socketTextStream("localhost", 9000);

        // 将输入的字符串数字转换成一元组的 integer 类型，并指定返回类型，（因为 java 的 dataset.lambda 表达式会擦出变量的属性，所以需要显示的标注返回值类型）
        DataStream<Tuple1<Integer>> tuple1DataStream = streamSource
                .map(v -> Tuple1.of(Integer.parseInt(v)))
                .returns(Types.TUPLE(Types.INT));

        // 使用 flatMap 转换输入的内容
        DataStream<Tuple2<String, Integer>> tuple2DataStream = streamSource
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> out.collect(new Tuple2<>("累加和：", Integer.parseInt(value))))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 按元组的第一位分组，并记录累加和
        tuple1DataStream.keyBy(0)
                .reduce((value1, value2) -> new Tuple1<>(value1.f0 + value2.f0))
                .print();

        // 按元组的第一位分组，并记录累加和
        tuple2DataStream.keyBy(0)
                .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1))
                .print();

        // 执行程序
        see.execute();
    }
}

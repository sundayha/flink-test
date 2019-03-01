package iteration;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class IterationT {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Long> dataSource = environment.generateSequence(0, 3);
        IterativeStream<Long> iterativeStream = dataSource.iterate();
        DataStream<Long> minusOne = iterativeStream.map(v -> v + 1);
        iterativeStream.closeWith(minusOne.filter(v -> v > 100));
        DataStream<Long> lessThanZero = minusOne.filter(v -> v > 0);
        lessThanZero.print();
        environment.execute();
    }
}

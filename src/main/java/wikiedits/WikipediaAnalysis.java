package wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(WikipediaEditEvent::getUser);

        DataStream<Tuple5<String, String, String, String, Long>> result1 = keyedEdits
                .timeWindow(Time.seconds(5)).aggregate(new AggregateFunction<WikipediaEditEvent, Tuple5<String, String, String, String, Long>, Tuple5<String, String, String, String, Long>>() {

                    @Override
                    public Tuple5<String, String, String, String, Long> createAccumulator() {
                        return new Tuple5<>("","","","", 0L);
                    }

                    @Override
                    public Tuple5<String, String, String, String, Long> add(WikipediaEditEvent value, Tuple5<String, String, String, String, Long> accumulator) {
                        accumulator.f0 = value.getUser();
                        accumulator.f1 = value.getTitle();
                        accumulator.f2 = value.getChannel();
                        accumulator.f3 = value.getSummary();
                        accumulator.f4 += value.getByteDiff();
                        return accumulator;
                    }

                    @Override
                    public Tuple5<String, String, String, String, Long> getResult(Tuple5<String, String, String, String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple5<String, String, String, String, Long> merge(Tuple5<String, String, String, String, Long> a, Tuple5<String, String, String, String, Long> b) {
                        a.f0 += b.f0;
                        a.f1 += b.f1;
                        a.f2 += b.f2;
                        a.f3 += b.f3;
                        a.f4 += b.f4;
                        return a;
                    }
                });


        result1.print();
        //result1.print();
        see.execute();
    }
}

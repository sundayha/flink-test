package dataset;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class IterationT {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);
        //bulkIterationT(environment);
        deltaIterationT(environment);
    }


    /**
     * 迭代
     * @param environment
     * @throws Exception
     */
    public static void bulkIterationT(ExecutionEnvironment environment) throws Exception {

        // 数据集中每一个值都循环2次
        IterativeDataSet<Long> longDataSet = environment.generateSequence(1, 5).iterate(2);

        // 循环时的操作
        DataSet<Long> longDataSet1 = longDataSet.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value + 1;
            }
        });

        // 结束循环
        DataSet<Long> out = longDataSet.closeWith(longDataSet1);

        out.print();
    }

    /**
     * 增量迭代 （示例无效）
     * @param environment
     * @throws Exception
     */
    public static void deltaIterationT(ExecutionEnvironment environment) throws Exception {

        DataSet<String> dataSet = environment.fromElements("叫", "我", "超", "人");
        DataSet<String> dataSet1 = environment.fromElements("超", "人", "啊");

        DataSet<Tuple1<String>> tuple1DataSet = dataSet.flatMap(new FlatMapFunction<String, Tuple1<String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
                out.collect(new Tuple1<>(value));
            }
        });

        DataSet<Tuple1<String>> tuple1DataSet1 = dataSet1.flatMap(new FlatMapFunction<String, Tuple1<String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
                out.collect(new Tuple1<>(value));
            }
        });

        int maxIterations = 100;
        int keyPosition = 0;

        DeltaIteration<Tuple1<String>, Tuple1<String>> deltaIteration =  tuple1DataSet.iterateDelta(tuple1DataSet1, maxIterations, keyPosition);

        DataSet<Tuple1<String>> workset = deltaIteration.getWorkset().groupBy(0).reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple1<String>>() {
            @Override
            public void reduce(Iterable<Tuple1<String>> values, Collector<Tuple1<String>> out) throws Exception {
                values.forEach(s-> {
                    out.collect(new Tuple1<>(s.f0.concat(" lala")));
                });
            }
        });

        deltaIteration.getSolutionSet().map(new MapFunction<Tuple1<String>, Object>() {
            @Override
            public Object map(Tuple1<String> value) throws Exception {

                System.out.println("SolutionSet: " + value);
                return value;
            }
        });

        DataSet<Tuple1<String>> tSolutionSet =  workset.join(deltaIteration.getSolutionSet()).where(0).equalTo(0).with(new JoinFunction<Tuple1<String>, Tuple1<String>, Tuple1<String>>() {
            @Override
            public Tuple1<String> join(Tuple1<String> first, Tuple1<String> second) throws Exception {
                System.out.println("first: " + first.f0);
                //System.out.println("second: " + second.f0);
                return first;
            }
        });

        DataSet<Tuple1<String>> nextWorkset = tSolutionSet.filter(new FilterFunction<Tuple1<String>>() {
            @Override
            public boolean filter(Tuple1<String> value) throws Exception {
                System.out.println("nextWorkset: " + value);
                return false;
            }
        });

        deltaIteration.closeWith(tSolutionSet, nextWorkset).print();


    }
}

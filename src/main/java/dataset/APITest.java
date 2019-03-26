package dataset;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;
import stream.eventtime.WatermarkTest;

import java.util.Arrays;
import java.util.List;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class APITest {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(3);

        DataSet<String> text = environment.fromElements("i love you ! you not love me !");

        DataSet<String> text1 = environment.fromElements("fuck you ! love me !");

        // test 转2元组
        DataSet<Tuple2<String, Integer>> tuple2DataSet = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String s : strings) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });

        // test1 转2元组
        DataSet<Tuple2<String, Integer>> tuple2DataSet1 = text1.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String s : strings) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });

        DataSet<Tuple2<String, Long>> sortDataSet = text.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String s : strings) {
                    Thread.sleep(100);
                    out.collect(new Tuple2<>(s, System.currentTimeMillis()));
                }
            }
        });

        //sortT(sortDataSet);
        zipT(tuple2DataSet);
    }

    /**
     * 累加字符个数
     * @param tuple2DataSet
     * @throws Exception
     */
    public static void wordCountT(DataSet<Tuple2<String, Integer>> tuple2DataSet) throws Exception {
        tuple2DataSet.groupBy(0).sum(1).print();
    }

    /**
     * 去重
     * @param tuple2DataSet
     * @throws Exception
     */
    public static void distinctT(DataSet<Tuple2<String, Integer>> tuple2DataSet) throws Exception {
        List<Tuple2<String, Integer>> list = tuple2DataSet.distinct().collect();

        list.forEach(s -> {
            System.out.println(s.f0);
        });
    }

    /**
     * join 操作， 条件为左侧数据集中，元组第一位，与其右侧数据集中第一位相等。然后输出结果
     * @param tuple2DataSet
     * @param tuple2DataSet1
     * @throws Exception
     */
    public static void joinT(DataSet<Tuple2<String, Integer>> tuple2DataSet, DataSet<Tuple2<String, Integer>> tuple2DataSet1) throws Exception {
        DataSet<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> dataSet = tuple2DataSet.join(tuple2DataSet1)
                .where(0)
                .equalTo(0).with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>>() {
                    @Override
                    public Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        System.out.println("first: " + first.f0);
                        System.out.println("second: " + second.f0);
                        return new Tuple2<>(first, second);
                    }
                });


        List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> tuple2s = dataSet.collect();

        System.out.println(Arrays.toString(tuple2s.toArray()));
    }

    /**
     * leftOuterJoin 操作时。 second 可能为 null，
     * rightOuterJoin 操作时。first 可能为 null，
     * fullOuterJoin 操作时。 first 或者 second 都可能为 null
     * @param tuple2DataSet
     * @param tuple2DataSet1
     */
    public static void leftOuterJoinT(DataSet<Tuple2<String, Integer>> tuple2DataSet, DataSet<Tuple2<String, Integer>> tuple2DataSet1) throws Exception {
        DataSet<String> stringDataSet = tuple2DataSet.leftOuterJoin(tuple2DataSet1)
                .where(0)
                .equalTo(0).with(new FlatJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public void join(Tuple2<String, Integer> first, Tuple2<String, Integer> second, Collector<String> out) throws Exception {
                        out.collect(first.f0.concat(" ").concat(second == null ? "null" : second.f0));
                    }
                });

        stringDataSet.print();
    }

    /**
     * coGroup 可以使两个数据集合并为一个数据集
     * @param tuple2DataSet
     * @param tuple2DataSet1
     * @throws Exception
     */
    public static void coGroupT(DataSet<Tuple2<String, Integer>> tuple2DataSet, DataSet<Tuple2<String, Integer>> tuple2DataSet1) throws Exception {
        DataSet<Tuple2<String, Integer>> stringDataSet = tuple2DataSet.coGroup(tuple2DataSet1).where(0).equalTo(0).with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple2<String, Integer>> out) throws Exception {
                first.forEach(out::collect);
                second.forEach(out::collect);
            }
        });
        stringDataSet.print();
        stringDataSet.groupBy(0).sum(1).print();
    }

    /**
     * cross 笛卡尔积，创建所有元素的元素对
     * @param tuple2DataSet
     * @param tuple2DataSet1
     * @throws Exception
     */
    public static void crossT(DataSet<Tuple2<String, Integer>> tuple2DataSet, DataSet<Tuple2<String, Integer>> tuple2DataSet1) throws Exception {
        tuple2DataSet.cross(tuple2DataSet1).print();
    }

    /**
     * union 并集，二合为一
     * @param tuple2DataSet
     * @param tuple2DataSet1
     * @throws Exception
     */
    public static void unionT(DataSet<Tuple2<String, Integer>> tuple2DataSet, DataSet<Tuple2<String, Integer>> tuple2DataSet1) throws Exception {
        tuple2DataSet.union(tuple2DataSet1).print();
    }

    /**
     * rebalance 重新进行平衡消除数据偏差。只有类似 map 这样的操作可能会遵循这个操作
     * @param tuple2DataSet
     * @throws Exception
     */
    public static void rebalanceT(DataSet<Tuple2<String, Integer>> tuple2DataSet) throws Exception {
        DataSet<String> dataSet = tuple2DataSet.rebalance().map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {

                return value.f0;
            }
        });

        dataSet.print();
    }

    /**
     * partitionByHash 对 key 进行 hash 分区。注意，此操作可能会需要大量时间
     * @param tuple2DataSet
     * @throws Exception
     */
    public static void partitionByHashT(DataSet<Tuple2<String, Integer>> tuple2DataSet) throws Exception {
        DataSet<String> dataSet = tuple2DataSet.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, String>() {
            @Override
            public void mapPartition(Iterable<Tuple2<String, Integer>> values, Collector<String> out) throws Exception {
                values.forEach(s -> out.collect(s.f0));
            }
        });

        dataSet.print();
    }

    /**
     * sortGroup 分组后进行排序
     * @param sortDataSet
     * @throws Exception
     */
    public static void sortT(DataSet<Tuple2<String, Long>> sortDataSet) throws Exception {
        DataSet<Tuple2<String, String>> dataSet = sortDataSet.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup(new GroupReduceFunction<Tuple2<String, Long>, Tuple2<String, String>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Long>> values, Collector<Tuple2<String, String>> out) throws Exception {
                values.forEach(s -> {
                    out.collect(new Tuple2<>(s.f0, WatermarkTest.toStrDate(s.f1)));
                });
            }
        });

        dataSet.print();

        sortDataSet.sortPartition(1, Order.ASCENDING).print();
    }

    /**
     * project 只能用于元组的转换，例如换个位置
     * @param tuple2DataSet
     * @throws Exception
     */
    public static void projectT(DataSet<Tuple2<String, Integer>> tuple2DataSet) throws Exception {
        DataSet<Tuple2<String, String>> dataSet = tuple2DataSet.project(1, 0);

        dataSet.print();
        System.out.println("dataset");
        dataSet.maxBy(1).print();
        System.out.println("dataset max");
        dataSet.minBy(1).print();

    }

    public static void zipT(DataSet<Tuple2<String, Integer>> tuple2DataSet) throws Exception {
        DataSet<Tuple2<Long, Tuple2<String, Integer>>> dataSet = DataSetUtils.zipWithIndex(tuple2DataSet);
        DataSetUtils.zipWithUniqueId(dataSet).print();
        //dataSet.print();
    }
}

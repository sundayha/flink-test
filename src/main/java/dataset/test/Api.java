package dataset.test;

import dataset.test.model.Rating;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class Api {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(3);

        PojoTypeInfo<Rating> pojoTypeInfo = (PojoTypeInfo<Rating>) TypeExtractor.createTypeInfo(Rating.class);
        List<Rating> ratings = new ArrayList<>(3);
        Rating rating = new Rating();
        Rating rating1 = new Rating();
        Rating rating2 = new Rating();
        rating.setName("李雷");
        rating1.setName("韩梅梅");
        rating2.setName("郭靖");
        rating.setCategory("男");
        rating1.setCategory("女");
        rating2.setCategory("男");
        rating.setPoints(50);
        rating1.setPoints(10);
        rating2.setPoints(100);
        ratings.add(rating);
        ratings.add(rating1);
        ratings.add(rating2);

        DataSet<Rating> ratingss  = environment.fromCollection(ratings, pojoTypeInfo);

        DataSet<Tuple2<String, Double>> weights = environment.fromElements(new Tuple2<>("男", 4.0), new Tuple2<>("女", 2.0));

        crossT(ratingss, weights);
    }

    /**
     * join 测试，Rating 对象的属性 category 与 weights 的元组的第一个值作为条件进行 join
     * @param ratings
     * @param weights
     * @throws Exception
     */
    public static void joinFunctionT(DataSet<Rating> ratings, DataSet<Tuple2<String, Double>> weights) throws Exception {

        DataSet<Tuple2<String, Double>> tuple2DataSet = ratings.join(weights)
                .where("category")
                .equalTo("f0")
                .with(new JoinFunction<Rating, Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> join(Rating first, Tuple2<String, Double> second) throws Exception {
                        return new Tuple2<>(first.getName(), first.getPoints() * second.f1);
                    }
                });

        tuple2DataSet.print();
    }

    /**
     * cross 交叉相乘
     * @param ratings
     * @param weights
     * @throws Exception
     */
    public static void crossT(DataSet<Rating> ratings, DataSet<Tuple2<String, Double>> weights) throws Exception {

        //DataSet<Tuple2<Rating, Double>> dataSet = ratings.cross(weights).projectFirst().projectSecond(1,0).project(1, 2, 0);

        DataSet<Tuple3<String, String, Double>> dataSet1 = ratings.cross(weights).with(new CrossFunction<Rating, Tuple2<String, Double>, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> cross(Rating val1, Tuple2<String, Double> val2) throws Exception {

                return new Tuple3<>(val1.name, val1.category, val1.points * val2.f1);
            }
        });

        dataSet1.print();
    }
}

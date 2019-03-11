package dataset.test;

import dataset.test.model.Rating;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.ArrayListSerializer;

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
        ratings.add(new Rating("李雷", "男", 50));
        ratings.add(new Rating("韩梅梅", "女", 10));
        ratings.add(new Rating("郭靖", "男", 50));

        ArrayListSerializer<Rating> arrayListSerializer = new ArrayListSerializer<>();

        CollectionInputFormat<Rating> collectionInputFormat = new CollectionInputFormat<>(ratings, arrayListSerializer);
        DataSet<Rating> ratingss  = environment.fromCollection(ratings, pojoTypeInfo);

        //environment.createInput(, pojoTypeInfo);
        //DataSet<Rating> ratings = environment.fromElements(Rating.class, new Rating("李雷", "男", 50), new Rating("", "女", 10), new Rating("郭靖", "男", 100));

        DataSet<Tuple2<String, Double>> weights = environment.fromElements(new Tuple2<>("男", 1.0), new Tuple2<>("女", 2.0));

        joinFunctionT(ratingss, weights);
    }

    public static void joinFunctionT(DataSet<Rating> ratings, DataSet<Tuple2<String, Double>> weights) throws Exception {
        //
        DataSet<Tuple2<String, Double>> tuple2DataSet = ratings.join(weights)
                .where("category")
                .equalTo("f0")
                .with(new JoinFunction<Rating, Tuple2<String, Double>, Tuple2<String, Double>>() {
                        @Override
                        public Tuple2<String, Double> join(Rating first, Tuple2<String, Double> second) throws Exception {
                            return new Tuple2<>(first.name, first.points * second.f1);
                        }
                    });

        tuple2DataSet.print();

        ratings.print();

    }
}

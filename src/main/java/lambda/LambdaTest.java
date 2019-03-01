package lambda;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class LambdaTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(1, 2, 3).map(i -> Tuple2.of(i, i)).returns(Types.TUPLE(Types.INT, Types.INT)).print();
    }
}

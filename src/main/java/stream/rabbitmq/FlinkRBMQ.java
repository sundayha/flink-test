package stream.rabbitmq;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class FlinkRBMQ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("192.168.0.126")
                .setPort(5672)
                .setUserName("admin")
                .setPassword("admin")
                .setVirtualHost("/")
                .build();

        DataStream<String> stream = env.addSource(new RMQSource<>(
                connectionConfig,
                "hello",
                true,
                new SimpleStringSchema())).setParallelism(1);

        DataStream<List<FindWaferEventTableVO>> listDataStream = stream.map(new MapFunction<String, List<FindWaferEventTableVO>>() {
            @Override
            public List<FindWaferEventTableVO> map(String value) throws Exception {
                List<FindWaferEventTableVO> eventTableVOS = new ArrayList<>();

                Gson gson = new Gson();
                JsonParser parser = new JsonParser();
                JsonArray jsonArray = parser.parse(value).getAsJsonArray();
                for (int i = 0; i < jsonArray.size(); i++) {
                    eventTableVOS.add(gson.fromJson(jsonArray.get(i), FindWaferEventTableVO.class));
                }
                //throw new RuntimeException("");
                return eventTableVOS;
            }
        });

        listDataStream.print();
        //listDataStream.addSink()
        env.execute();
    }

}

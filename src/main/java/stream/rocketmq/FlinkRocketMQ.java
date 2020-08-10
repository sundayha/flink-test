package stream.rocketmq;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSink;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.selector.DefaultTopicSelector;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueSerializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class FlinkRocketMQ {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "192.168.0.126:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "novaDeepTest1");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "consumer_Fdc_Group");

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "192.168.0.126:9876");

        DataStream<HashMap> msg =  env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps))
                .name("rocketmq-source")
                .setParallelism(2)
                .process(new ProcessFunction<Map, Map>() {
                    @Override
                    public void processElement(Map in, Context ctx, Collector<Map> out) throws Exception {

                        //Message message = new Message();
                        //String[] strings = in.split(",");
                        //message.setDate(Long.parseLong(strings[0]));
                        //message.setMsg(strings[1]);
                        //message.setDateTime(strings[2]);
                        //message.setTopic(strings[3]);
                        HashMap result = new HashMap();
                        result.put("id", "1");
                        String[] arr = in.get("address").toString().split("\\s+");
                        result.put("province", arr[arr.length-1]);
                        out.collect(result);
                    }
                }).returns(HashMap.class);

       msg.addSink(new RocketMQSink(new SimpleKeyValueSerializationSchema("id", "province"),
               new DefaultTopicSelector("novaDeepTest2"), producerProps).withBatchFlushOnCheckpoint(true))
               .name("rocketmq-sink")
               .setParallelism(2);

        msg.print();
        try {
            env.execute("rocketmq-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

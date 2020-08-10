package stream.rabbitmq;


import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class Send {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.0.126");
        factory.setUsername("admin");
        factory.setPassword("admin");
        try (Connection connection = factory.newConnection()
             ) {
            String corrId = java.util.UUID.randomUUID().toString();
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder().correlationId(corrId).build();

            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, "fanoutExchange", "");

            String message = "[{\"fWaferID\":18929,\"moduleName\":\"NIM-07_DEP\",\"deviceName\":\"GST9449R8\",\"lotId\":\"GF1904005B\",\"recipe\":\"SP08-55R3\",\"waferId\":\"GF1904005B.23\",\"startTime\":\"Apr 2, 2019 8:55:56 PM\",\"endTime\":\"Apr 2, 2019 9:01:24 PM\"},{\"fWaferID\":18930,\"moduleName\":\"NIM-07_DEP\",\"deviceName\":\"GST9449R8\",\"lotId\":\"GF1904004A\",\"recipe\":\"SP08-55R3\",\"waferId\":\"GF1904004A.03\",\"startTime\":\"Apr 2, 2019 9:01:26 PM\",\"endTime\":\"Apr 2, 2019 9:04:06 PM\"},{\"fWaferID\":18931,\"moduleName\":\"NIM-07_DEP\",\"deviceName\":\"GST9449R8\",\"lotId\":\"GF1904004A\",\"recipe\":\"SP08-55R3\",\"waferId\":\"GF1904004A.01\",\"startTime\":\"Apr 2, 2019 9:02:37 PM\",\"endTime\":\"Apr 2, 2019 9:08:06 PM\"}]";
            channel.basicPublish("fanoutExchange", QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
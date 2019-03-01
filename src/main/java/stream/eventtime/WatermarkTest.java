package stream.eventtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class WatermarkTest {

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019/3/1 9:41 AM
     * @param date 毫秒
     * @apiNote 毫秒转日期字符串
     * @return string
     */
    private static String toStrDate(Long date) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS");
        return dateFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(date), ZoneId.systemDefault()));
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 运行环境设置为事件时间
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        environment.setParallelism(2);

        // 从 socket 中读取消息
        DataStream<String> socketDataStream = environment.socketTextStream("localhost", 9000);

        // 转换 message 对象
        DataStream<Message> messageDataStream = socketDataStream.map(new MapFunction<String, Message>() {
            @Override
            public Message map(String value) throws Exception {
                Message message = new Message();
                String[] strings = value.split(",");
                message.setDate(Long.parseLong(strings[0]));
                message.setMsg(strings[1]);
                message.setDateTime(strings[2]);
                message.setTopic(strings[3]);
                return message;
            }
        });

        // 设置水印
        DataStream<Message> waterMarkDataStream = messageDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Message>() {

            // 延迟时间
            Long delay = 3500L;
            // 当前最大时间戳
            Long currentMaxTimestamp = 0L;
            // 水位
            Watermark watermark;

            @Override
            public Watermark getCurrentWatermark() {
                // 取事件时间的最大值 - 延迟时间的差作为水位的参考
                watermark = new Watermark(currentMaxTimestamp - delay);
                return watermark;
            }

            @Override
            public long extractTimestamp(Message element, long previousElementTimestamp) {
                currentMaxTimestamp = Math.max(element.getDate(), currentMaxTimestamp);
                System.out.println("线程号：" + Thread.currentThread().getId() +
                        " | 当前最大时间戳：".concat(toStrDate(currentMaxTimestamp))
                                .concat(" | 事件时间：").concat(toStrDate(element.getDate()))
                                .concat(" | 水印日期：").concat(toStrDate(watermark.getTimestamp())
                                .concat(" | 水印：")).concat(watermark.toString())
                                .concat(" | 消息：").concat(element.getMsg())
                                .concat(" | 是否延迟：").concat(currentMaxTimestamp > element.getDate() ? "是" : "否")
                );
                return element.getDate();
            }
        });

         // window 时间, 基于事件时间
         // [0, 3] [3, 6] [6, 9] [9, 12] ..... [xx, xx]
         // 例如事件时间 [13:08:05.6770, 13:08:04.5860], 它们会落在 [13:08:03, 13:08:06] 的 window 中
         // 当 currentMaxTimestamp 大于 window-end-time 时窗口被触发
         waterMarkDataStream.keyBy("topic").timeWindow(Time.seconds(3)).apply(new WindowFunction<Message, Object, Tuple, TimeWindow>() {
             @Override
             public void apply(Tuple tuple, TimeWindow window, Iterable<Message> input, Collector<Object> out) throws Exception {
                 for (Message message : input) {
                     out.collect(
                             "结果输出 -> 线程号：".concat(String.valueOf(Thread.currentThread().getId()))
                                     .concat(" ").concat(message.toString())
                                     .concat(" | 窗口开启：").concat(toStrDate(window.getStart()))
                                     .concat(" | 窗口关闭：").concat(toStrDate(window.getEnd()))
                     );
                 }
             }
         }).print();

        // 1551416885677,是,2019-03-01 13:08:05.6770,test 2
        // 1551416884586,我,2019-03-01 13:08:04.5860,test 1
        // 1551416888683,爸,2019-03-01 13:08:08.6830,test 5
        // 1551416887680,爸,2019-03-01 13:08:07.6800,test 4
        // 1551416886679,你,2019-03-01 13:08:06.6790,test 3

        // 1551416885677,是,2019-03-01 13:08:05.6770,test
        // 1551416884586,我,2019-03-01 13:08:04.5860,test
        // 1551416888683,爸,2019-03-01 13:08:08.6830,test
        // 1551416887680,爸,2019-03-01 13:08:07.6800,test
        // 1551416886679,你,2019-03-01 13:08:06.6790,test
        // 1559416886679,测,2019-06-02 03:21:26.6790,test
        // 1569416886679,试,2019-09-25 21:08:06.6790,test
        environment.execute();
    }
}

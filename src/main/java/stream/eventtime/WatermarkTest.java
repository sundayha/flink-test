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

        // 在并发的场景下，如果窗口有数据，并且每条线程的 water mark 都要大于 window end time 这时候窗口才会被触发。
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
            Long delay = 0L;
            // 当前最大时间戳
            Long currentMaxTimestamp = 0L;

            @Override
            public Watermark getCurrentWatermark() {
                // 取事件时间的最大值 - 延迟时间的差作为水位的参考
                return new Watermark(currentMaxTimestamp - delay);
            }

            @Override
            public long extractTimestamp(Message element, long previousElementTimestamp) {
                currentMaxTimestamp = Math.max(element.getDate(), currentMaxTimestamp);
                System.out.println("线程号：" + Thread.currentThread().getId() +
                        " | 当前最大时间戳：".concat(toStrDate(currentMaxTimestamp))
                                .concat(" | 事件时间：").concat(toStrDate(element.getDate()))
                                .concat(" | 水印日期：").concat(toStrDate(getCurrentWatermark().getTimestamp()))
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

        /**
         * 结果数据
         */
        //线程号：55 | 当前最大时间戳：2019-03-01 13:08:05.6770 | 事件时间：2019-03-01 13:08:05.6770 | 水印日期：2019-03-01 13:08:05.6770 | 消息：是 | 是否延迟：否
        //线程号：53 | 当前最大时间戳：2019-03-01 13:08:04.5860 | 事件时间：2019-03-01 13:08:04.5860 | 水印日期：2019-03-01 13:08:04.5860 | 消息：我 | 是否延迟：否
        //线程号：55 | 当前最大时间戳：2019-03-01 13:08:08.6830 | 事件时间：2019-03-01 13:08:08.6830 | 水印日期：2019-03-01 13:08:08.6830 | 消息：爸 | 是否延迟：否
        //线程号：53 | 当前最大时间戳：2019-03-01 13:08:07.6800 | 事件时间：2019-03-01 13:08:07.6800 | 水印日期：2019-03-01 13:08:07.6800 | 消息：爸 | 是否延迟：否
        //2> 结果输出 -> 线程号：57 主题：test 日期：2019-03-01 13:08:05.6770 毫秒：1551416885677 消息：是 | 窗口开启：2019-03-01 13:08:03.0000 | 窗口关闭：2019-03-01 13:08:06.0000
        //2> 结果输出 -> 线程号：57 主题：test 日期：2019-03-01 13:08:04.5860 毫秒：1551416884586 消息：我 | 窗口开启：2019-03-01 13:08:03.0000 | 窗口关闭：2019-03-01 13:08:06.0000
        //线程号：55 | 当前最大时间戳：2019-03-01 13:08:08.6830 | 事件时间：2019-03-01 13:08:06.6790 | 水印日期：2019-03-01 13:08:08.6830 | 消息：你 | 是否延迟：是
        //线程号：53 | 当前最大时间戳：2019-06-02 03:21:26.6790 | 事件时间：2019-06-02 03:21:26.6790 | 水印日期：2019-06-02 03:21:26.6790 | 消息：测 | 是否延迟：否
        //线程号：55 | 当前最大时间戳：2019-09-25 21:08:06.6790 | 事件时间：2019-09-25 21:08:06.6790 | 水印日期：2019-09-25 21:08:06.6790 | 消息：试 | 是否延迟：否
        //2> 结果输出 -> 线程号：57 主题：test 日期：2019-03-01 13:08:08.6830 毫秒：1551416888683 消息：爸 | 窗口开启：2019-03-01 13:08:06.0000 | 窗口关闭：2019-03-01 13:08:09.0000
        //2> 结果输出 -> 线程号：57 主题：test 日期：2019-03-01 13:08:07.6800 毫秒：1551416887680 消息：爸 | 窗口开启：2019-03-01 13:08:06.0000 | 窗口关闭：2019-03-01 13:08:09.0000
        //2> 结果输出 -> 线程号：57 主题：test 日期：2019-03-01 13:08:06.6790 毫秒：1551416886679 消息：你 | 窗口开启：2019-03-01 13:08:06.0000 | 窗口关闭：2019-03-01 13:08:09.0000
    }
}

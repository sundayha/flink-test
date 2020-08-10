package stream.eventtime;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;

/**
 * @author 张博【zhangb@lianliantech.cn】
 *
 * nc -lk 9000
 */
public class SlidingEventTimeWindowsTest {

    /**
     * 创建人：张博【zhangb@novadeep.com】
     * 时间：2019/3/1 9:41 AM
     * @param date 毫秒
     * @apiNote 毫秒转日期字符串
     * @return string
     */
    public static String toStrDate(Long date) {
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
        DataStream<Message> waterMarkDataStream = messageDataStream.assignTimestampsAndWatermarks(new WatermarkStrategy<Message>() {
            private static final long serialVersionUID = 1000616117638840510L;
            // 延迟时间
            long delay = 1000;
            String info;

            @Override
            public WatermarkGenerator<Message> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Message>() {
                    @Override
                    public void onEvent(Message event, long eventTimestamp, WatermarkOutput output) {
                        Watermark watermark = new Watermark(eventTimestamp - delay);
                        System.out.println(info.concat(" | 水印日期：").concat(watermark.getFormattedTimestamp()));
                        output.emitWatermark(watermark);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                    }
                };
            }

            @Override
            public TimestampAssigner<Message> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (element, recordTimestamp) -> {
                    info = "输入 -> 线程号：".concat(String.valueOf(Thread.currentThread().getId()))
                            .concat(" | 事件时间：")
                            .concat(toStrDate(element.getDate()))
                            .concat(" | 消息：").concat(element.getMsg());
                    return element.date;
                };
            }
        });

        // 侧输出迟到数据
        OutputTag<Message> outputTag = new OutputTag<Message>("side-output"){};


        // window 时间, 基于事件时间。窗口时间为 3，窗口的开始结束时间为，例如
        // [13:08:00.000, 13:08:03.000), [13:08:03.000, 13:08:06.000), [13:08:06.000, 13:08:09.000) [13:08:09.000, 13:08:12.000]
        // 例如事件时间 [13:08:05.6770, 13:08:04.5860], 它们会落在 [13:08:03, 13:08:06] 的 window 中
        // 当水印时间大于 window-end-time 时窗口被触发，然后在窗口中的数据会被统计计算。
        // 如果设置并行度的话，需要每一个线程中的水印时间大于 window-end-time 时窗口才会被触发！！！
        SingleOutputStreamOperator<String> window = waterMarkDataStream
                .keyBy("topic")
                .timeWindow(Time.seconds(3), Time.seconds(2))
                // 仅对窗口的延迟。延迟的数据的水印时间没在当前窗口的时间范围内，该数据将会被丢弃。
                // 使窗口延迟，可以捕获延迟的数据。再进行处理。
                // 延迟事件时间水印时间在 end-of-window + allowedLateness 时间范围内，可以重新触发窗口进行计算。
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Message, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Message> input, Collector<String> out) {
                        List<Message> messages = IteratorUtils.toList(input.iterator());
                        messages.stream()
                                .sorted(Comparator.comparing(Message::getDate))
                                .forEach(message -> {
                                    out.collect(
                                            "结果输出 -> 线程号：".concat(String.valueOf(Thread.currentThread().getId()))
                                                    .concat(" 窗口开启：").concat(toStrDate(window.getStart()))
                                                    .concat(" | 窗口关闭：").concat(toStrDate(window.getEnd()))
                                                    .concat(" | ").concat(message.toString())
                                    );
                                });
                    }
                });

        DataStream<Message> outputTagDataStream = window.getSideOutput(outputTag);

        // 迟到数据处理
        outputTagDataStream.process(new ProcessFunction<Message, Message>() {
            private static final long serialVersionUID = -2068307477944675699L;
            @Override
            public void processElement(Message value, Context ctx, Collector<Message> out) {
                out.collect(value);
                //ctx.output(outputTag, value);
                //ctx.output(outputTag, value);
            }
        }).print();

        window.print();

        //1551416881677,我第一个窗口输出,2019-03-01 13:08:01.6770,test
        //1551416885677,是,2019-03-01 13:08:05.6770,test
        //1551416884586,你,2019-03-01 13:08:04.5860,test
        //1551416888683,子,2019-03-01 13:08:08.6830,test
        //1551416887680,儿,2019-03-01 13:08:07.6800,test
        //1551416886679,我,2019-03-01 13:08:06.6790,test
        //1559416886679,测,2019-06-02 03:21:26.6790,test
        //1569416886679,试,2019-09-25 21:08:06.6790,test
        //1551416885679,超过窗口时间的消息,2019-03-01 13:08:05.6790,test


        // 并行度为 2 时窗口统计的结果
        //输入 -> 线程号：68 | 事件时间：2019-03-01 13:08:01.6770 | 消息：我第一个窗口输出 | 水印日期：2019-03-01 13:08:00.677
        //输入 -> 线程号：67 | 事件时间：2019-03-01 13:08:05.6770 | 消息：是 | 水印日期：2019-03-01 13:08:04.677
        //输入 -> 线程号：68 | 事件时间：2019-03-01 13:08:04.5860 | 消息：你 | 水印日期：2019-03-01 13:08:03.586
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:00.0000 | 窗口关闭：2019-03-01 13:08:03.0000 | 主题：test 日期：2019-03-01 13:08:01.6770 毫秒：1551416881677 消息：我第一个窗口输出
        //输入 -> 线程号：67 | 事件时间：2019-03-01 13:08:08.6830 | 消息：子 | 水印日期：2019-03-01 13:08:07.683
        //输入 -> 线程号：68 | 事件时间：2019-03-01 13:08:07.6800 | 消息：儿 | 水印日期：2019-03-01 13:08:06.680
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:02.0000 | 窗口关闭：2019-03-01 13:08:05.0000 | 主题：test 日期：2019-03-01 13:08:04.5860 毫秒：1551416884586 消息：你
        //输入 -> 线程号：67 | 事件时间：2019-03-01 13:08:06.6790 | 消息：我 | 水印日期：2019-03-01 13:08:05.679
        //输入 -> 线程号：68 | 事件时间：2019-06-02 03:21:26.6790 | 消息：测 | 水印日期：2019-06-02 03:21:25.679
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:04.0000 | 窗口关闭：2019-03-01 13:08:07.0000 | 主题：test 日期：2019-03-01 13:08:04.5860 毫秒：1551416884586 消息：你
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:04.0000 | 窗口关闭：2019-03-01 13:08:07.0000 | 主题：test 日期：2019-03-01 13:08:05.6770 毫秒：1551416885677 消息：是
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:04.0000 | 窗口关闭：2019-03-01 13:08:07.0000 | 主题：test 日期：2019-03-01 13:08:06.6790 毫秒：1551416886679 消息：我
        //输入 -> 线程号：67 | 事件时间：2019-09-25 21:08:06.6790 | 消息：试 | 水印日期：2019-09-25 21:08:05.679
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:06.0000 | 窗口关闭：2019-03-01 13:08:09.0000 | 主题：test 日期：2019-03-01 13:08:06.6790 毫秒：1551416886679 消息：我
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:06.0000 | 窗口关闭：2019-03-01 13:08:09.0000 | 主题：test 日期：2019-03-01 13:08:07.6800 毫秒：1551416887680 消息：儿
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:06.0000 | 窗口关闭：2019-03-01 13:08:09.0000 | 主题：test 日期：2019-03-01 13:08:08.6830 毫秒：1551416888683 消息：子
        //2> 结果输出 -> 线程号：70 窗口开启：2019-03-01 13:08:08.0000 | 窗口关闭：2019-03-01 13:08:11.0000 | 主题：test 日期：2019-03-01 13:08:08.6830 毫秒：1551416888683 消息：子
        environment.execute();
    }
}

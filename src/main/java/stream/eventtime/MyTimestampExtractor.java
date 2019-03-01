package stream.eventtime;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class MyTimestampExtractor implements AssignerWithPeriodicWatermarks<Message> {

    private Long delay = 5000L;

    private Long currentMaxTimestamp = 0L;

    private Watermark watermark;

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

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        watermark = new Watermark(currentMaxTimestamp - delay);
        return watermark;
    }

    @Override
    public long extractTimestamp(Message element, long previousElementTimestamp) {
        currentMaxTimestamp = Math.max(element.getDate(), currentMaxTimestamp);
        System.out.println("时间戳：".concat(toStrDate(currentMaxTimestamp)).concat("事件时间：").concat(toStrDate(element.getDate())).concat("水印：").concat(watermark.toString()));
        return element.getDate();
    }

    public static void main(String[] args) throws InterruptedException {
        //System.out.println(toStrDate(System.currentTimeMillis()));
        String[] strings = "我,是,你,爸,爸".split(",");
        for (String s : strings) {
            Long aLong = System.currentTimeMillis();
            System.out.println(aLong + "," + s + "," + toStrDate(aLong) + ",test");
            Thread.sleep(1000);
        }
    }
}

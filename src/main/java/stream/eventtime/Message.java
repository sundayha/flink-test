package stream.eventtime;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class Message {
    public String topic;
    public Long date;
    public String msg;
    public String dateTime;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    @Override
    public String toString() {
        return "主题：" + getTopic() +" 日期：" + getDateTime() + " 毫秒：" + getDate() + " 消息：" + getMsg();
    }
}

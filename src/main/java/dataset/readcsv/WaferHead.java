package dataset.readcsv;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class WaferHead implements Comparable<WaferHead> {

    private Integer id;
    private String waferId;
    private String path;
    private String state;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getWaferId() {
        return waferId;
    }

    public void setWaferId(String waferId) {
        this.waferId = waferId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public int compareTo(WaferHead o) {
        return this.id.compareTo(o.id);
    }
}

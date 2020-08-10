package stream.rabbitmq;

import java.util.Date;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class FindWaferEventTableVO {
    private Long fWaferID;
    private String moduleName;
    private String deviceName;
    private String lotId;
    private String recipe;
    private String waferId;
    private Date startTime;
    private Date endTime;

    public String getRecipe() {
        return recipe;
    }

    public void setRecipe(String recipe) {
        this.recipe = recipe;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public Long getfWaferID() {
        return fWaferID;
    }

    public void setfWaferID(Long fWaferID) {
        this.fWaferID = fWaferID;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public String getLotId() {
        return lotId;
    }

    public void setLotId(String lotId) {
        this.lotId = lotId;
    }

    public String getWaferId() {
        return waferId;
    }

    public void setWaferId(String waferId) {
        this.waferId = waferId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
}

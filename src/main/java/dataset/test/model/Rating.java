package dataset.test.model;

import java.io.Serializable;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class Rating implements Serializable {

    private static final long serialVersionUID = 3323789775973406522L;
    public String name;
    public String category;
    public int points;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public int getPoints() {
        return points;
    }

    public void setPoints(int points) {
        this.points = points;
    }
}

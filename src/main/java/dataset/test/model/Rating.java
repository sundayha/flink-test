package dataset.test.model;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class Rating {
    public String name;
    public String category;
    public int points;

    public Rating(String name, String category, int points) {
        this.name = name;
        this.category = category;
        this.points = points;
    }

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

package hbase;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
@HBaseModel(tableName = "user")
public class User {

    public User() {}

    public User(String rowKey, String name, String sex, String age) {
        this.rowKey = rowKey;
        this.name = name;
        this.sex = sex;
        this.age = age;
    }

    @HBaseModelProperty(family = "rowKey", qualifier = "rowKey")
    private String rowKey;
    @HBaseModelProperty(family = "userInfo", qualifier = "name")
    private String name;
    @HBaseModelProperty(family = "userInfo", qualifier = "sex")
    private String sex;
    @HBaseModelProperty(family = "userInfo", qualifier = "age")
    private String age;
    //@HBaseModelProperty(family = "userInfo", qualifier = "hobby")
    //private List<String> hobby;

    //public List<String> getHobby() {
    //    return hobby;
    //}
    //
    //public void setHobby(List<String> hobby) {
    //    this.hobby = hobby;
    //}

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User".concat("rowkey: ").concat(getRowKey()).concat(", name: ").concat(getName()).concat(", age: ").concat(getAge()).concat(", sex: ").concat(getSex()) ;
    }
}

package hbase;

import org.junit.Test;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HBaseT {

    private User user = new User("101", "顶呱呱，跨啦啦", "男", "20");

    @Test
    public void createTableType1() {
        HBaseMapper.createTable("user", "information");
    }

    @Test
    public void createTableType2() {

        HBaseMapper.createTable(user);
    }

    @Test
    public void insert() {
        HBaseMapper.insert("user", user);
    }

    @Test
    public void getAllData() {
        HBaseMapper.getAllData("user", User.class)
                .forEach(
                        s -> {
                            System.out.println(s.getRowKey());
                            System.out.println(s.getName());
                            System.out.println(s.getSex());
                            System.out.println(s.getAge());
                            //System.out.println(s.toString());
                            System.out.println("==========");
                        }
                );
    }

    @Test
    public void getResultByRowKey() {
        HBaseMapper.getListByRowKey("user", "102", User.class).forEach(
                s -> {
                    System.out.println(s.getRowKey());
                    System.out.println(s.getName());
                    System.out.println(s.getAge());
                    System.out.println(s.getSex());
                }
        );
    }

    @Test
    public void deleteTable() {
        HBaseMapper.deleteTable("user");
    }
}

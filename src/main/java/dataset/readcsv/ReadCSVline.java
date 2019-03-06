package dataset.readcsv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class ReadCSVline {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> stringDataSet = environment.readTextFile("/Users/zhangbo/Downloads/ND_SVD_DEV_wafer_aoi_head.csv");
        List<String> aa = stringDataSet.filter(s -> s.startsWith("5")).collect();
        List<String> strings = stringDataSet.collect();
        strings.forEach(System.out::println);
        System.out.println(strings.size());
        aa.forEach(System.out::println);
        System.out.println(aa.size());
    }
}

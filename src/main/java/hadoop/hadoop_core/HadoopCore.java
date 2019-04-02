package hadoop.hadoop_core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class HadoopCore {

    public static void main(String[] args) {

    }

    private static void readAsTest() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/zhangbo/Documents/ideaProject/flink-test/src/main/resources/core-site.xml"));
        conf.addResource(new Path("/Users/zhangbo/Documents/ideaProject/flink-test/src/main/resources/hdfs-site.xml"));
        //conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem dst = FileSystem.get(conf);
        Path p = new Path("/user/zhangbo/SI10953-14_X_77_Y_55.jpg");
        FSDataInputStream iStream = dst.open(p);

        OutputStream os = new FileOutputStream("/Users/zhangbo/Downloads/flinkOut//SI10953-14_X_77_Y_55.jpg");

        byte[] buffer = new byte[400];
        int length = 0;
        while ((length = iStream.read(buffer)) > 0) {
            os.write(buffer, 0, length);
        }
        os.flush();
        os.close();
        iStream.close();
    }
}

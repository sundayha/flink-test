package hbase.pic;

import hbase.HBaseMapper;
import hbase.pic.model.SVDPic;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author 张博【zhangb@lianliantech.cn】
 */
public class Go {

    //private static

    public static void main(String[] args) {

        String[] strings = {"jpg"};
        FileUtils.listFiles(new File("/Users/zhangbo/Documents/ncaiData/aoi/deducing/bumping/HI6421V710WSB/CF1907332A/2AI029JM1/DHF447-12-G4/JPG"), strings, false)
                .forEach(s -> {

                    try {

                        SVDPic svdPic = new SVDPic();

                        svdPic.setRowKey(s.getName().split("\\.")[0]);

                        svdPic.setFile(FileUtils.readFileToByteArray(s.getAbsoluteFile()));
                        HBaseMapper.insert(svdPic);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

    }
}

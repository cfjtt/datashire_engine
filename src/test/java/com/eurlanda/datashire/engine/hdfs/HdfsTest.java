package com.eurlanda.datashire.engine.hdfs;

import com.eurlanda.datashire.engine.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by zhudebin on 16/5/20.
 */
public class HdfsTest {

    @Test
    public void testEmptyFile() {
        try {
            HdfsUtil.readFile("hdfs://192.168.137.131:8020/TestData/test3/student.txt/_SUCCESS");
//            HdfsUtil.readFile("hdfs://192.168.137.131:8020/TestData/test3/student.txt/part-20160520105153-00002");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testEmptyFile2() {
//        String filePath = "hdfs://192.168.137.131:8020/TestData/test3/student.txt/_SUCCESS";
        String filePath = "hdfs://192.168.137.131:8020/TestData/test3/student.txt/part-20160520105153-00002";
        Configuration conf = new Configuration();
        Path srcPath = new Path(filePath);

        System.out.println(srcPath.getName());

        InputStream in = null;
        try {
            FileSystem fs = srcPath.getFileSystem(conf);

            FileStatus[] fss = fs.globStatus(srcPath, new PathFilter() {
                @Override public boolean accept(Path path) {
                    return true;
                }
            });
            System.out.println(fss.length);
            System.out.println(fss[0].getLen());
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    @Test
    public void testString() {
        String str = "\\N,adsfas";
        String s1 = str.split(",")[0];
        System.out.println(s1);
        System.out.println(s1.getBytes().length);
    }

    @Test
    public void testHa() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ehadoop");
        conf.set("dfs.nameservices", "ehadoop");
        conf.set("dfs.ha.namenodes.ehadoop", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.ehadoop.nn1", "192.168.137.101:8020");
        conf.set("dfs.namenode.rpc-address.ehadoop.nn2", "192.168.137.102:8020");
        conf.set("dfs.client.failover.proxy.provider.hadoop2cluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            FileStatus[] list = fs.listStatus(new Path("/"));
            for (FileStatus file : list) {
                System.out.println(file.getPath().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

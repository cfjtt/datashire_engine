package com.eurlanda.datashire.engine.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhudebin on 16/1/19.
 */
public class HdfsUtil {

    //创建新文件
    public static void createFile(String dst , byte[] contents) throws IOException{
        Configuration conf = new Configuration();
        Path dstPath = new Path(dst); //目标路径
        FileSystem fs = dstPath.getFileSystem(conf);
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }

    //上传本地文件
    public static void uploadFile(String src,String dst) throws IOException {
        Configuration conf = new Configuration();
        Path srcPath = new Path(src); //原路径
        FileSystem fs = srcPath.getFileSystem(conf);
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false,srcPath, dstPath);

        /**
        //打印文件路径
        System.out.println("Upload to "+conf.get("fs.default.name"));
        System.out.println("------------list files------------"+"\n");
        FileStatus[] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus)
        {
            System.out.println(file.getPath());
        }
         */
        fs.close();
    }

    //文件重命名
    public static void rename(String oldName,String newName) throws IOException{
        Configuration conf = new Configuration();
        Path oldPath = new Path(oldName);
        FileSystem fs = oldPath.getFileSystem(conf);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if(isok){
            System.out.println("rename ok!");
        }else{
            System.out.println("rename failure");
        }
        fs.close();
    }
    //删除文件
    public static void delete(String filePath) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("delete ok!");
        }else{
            System.out.println("delete failure");
        }
        fs.close();
    }

    //创建目录
    public static void mkdir(String path) throws IOException{
        Configuration conf = new Configuration();
        Path srcPath = new Path(path);
        FileSystem fs = srcPath.getFileSystem(conf);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("create dir ok!");
        }else{
            System.out.println("create dir failure");
        }
        fs.close();
    }

    //读取文件的内容
    public static void readFile(String filePath) throws IOException{
        Configuration conf = new Configuration();
        Path srcPath = new Path(filePath);
        FileSystem fs = srcPath.getFileSystem(conf);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 列出文件夹下的所有文件
     * @param path 文件夹路径
     * @param reg 正则匹配
     * @param recursive 是否递归子目录
     * @return
     * @throws IOException
     */
    public static List<String> listFiles(String path, String reg, boolean recursive) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(path), recursive);
        List<String> files = new ArrayList<>();
        boolean filter = org.apache.commons.lang.StringUtils.isNotEmpty(reg);

        Pattern pattern = null;
        if(filter) {
            pattern = Pattern.compile(reg);
        }

        while(ri.hasNext()) {
            LocatedFileStatus lfs = ri.next();
            if(lfs.isFile()) {
                if(filter) {
                    Matcher matcher = pattern.matcher(lfs.getPath().getName());
                    if(!matcher.matches()) {
                        continue;
                    }
                }
                files.add(lfs.getPath().toString());
            }
        }

        return files;
    }

    /**
     * 将该路径设置全部的读写属性
     * @param path
     */
    public static void setAllACL(String path) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(path), conf);
            fs.setPermission(new Path(path), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

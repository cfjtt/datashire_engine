
package com.eurlanda.datashire.engine.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class CodecTest {
    //压缩文件
    public static void compress(String codecClassName,
            String sourceFile, String targetFile) throws Exception{
        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        //指定压缩文件路径
        FSDataOutputStream outputStream = fs.create(new Path(targetFile));
        //指定要被压缩的文件路径
        FSDataInputStream in = fs.open(new Path(sourceFile));
        //创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);  
        IOUtils.copyBytes(in, out, conf); 
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }
    
    //解压缩
    public static void uncompress(String codecClassName,
            String sourceFile, String targetFile) throws Exception{
        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        FSDataInputStream inputStream = fs.open(new Path(sourceFile));
         //把text文件里到数据解压，然后输出到控制台  
        InputStream in = codec.createInputStream(inputStream);  
        IOUtils.copyBytes(in, System.out, conf);
        IOUtils.closeStream(in);
    }
    
    //使用文件扩展名来推断二来的codec来对文件进行解压缩
    public static void uncompressBySuffix(String uri) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        
        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if(codec == null){
            System.out.println("no codec found for " + uri);
            System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally{
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }
    
    public static void main(String[] args) throws Exception {
        //compress("org.apache.hadoop.io.compress.GzipCodec");
        //uncompress("text");
        uncompressBySuffix("hdfs://ehadoop/eurlanda/test_data/sample_tree_data.gz");

        String gzip = "org.apache.hadoop.io.compress.GzipCodec";
        String lz4 = "org.apache.hadoop.io.compress.Lz4Codec";
        String bzip = "org.apache.hadoop.io.compress.BZip2Codec";
        String deflate = "org.apache.hadoop.io.compress.DeflateCodec";
        String snappy = "org.apache.hadoop.io.compress.SnappyCodec";

        /*
        compress(gzip,
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.csv",
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.gz");
        */

        // 需要本地库支持
        compress(lz4,
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.csv",
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.lz4");

        /*compress(bzip,
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.csv",
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.bz2");*/
        /*compress(deflate,
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.csv",
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.deflate");*/

        /*// 需要本地库支持
        compress(snappy,
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.csv",
                "hdfs://ehadoop/eurlanda/test_data/sample_tree_data.snappy");
        */
    }

}
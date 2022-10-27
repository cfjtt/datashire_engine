package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.hadoop.XmlStreamInputFormat;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import com.eurlanda.datashire.enumeration.EncodingType;
import com.eurlanda.datashire.enumeration.FileType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 14-5-12.
 * 从hdfs中获取数据
 */
public class THdfsSquid extends TSquid {

    private static Log log = LogFactory.getLog(THdfsSquid.class);

    // 将 host:port 统一到IP , since 2.0
    private String ip;
    private String hdfsUrl;
    private String[] paths;
    // 文件类型
    private String fileType;
    private FileType fileTypeEnum;
    //文件行分割信息
    private FileRecordSeparator fileLineSeparator;
    // connection squid id
    private Integer connectionSquidId;
    // 压缩格式 null代表不是压缩文件,非空代表压缩编码的名字
    private String codec = "_non_codec_";

    public Integer getConnectionSquidId() {
        return connectionSquidId;
    }

    public void setConnectionSquidId(Integer connectionSquidId) {
        this.connectionSquidId = connectionSquidId;
    }

    public THdfsSquid() {
        this.setType(TSquidType.HDFS_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException  {
        if (isFinished()) return null;
        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(), this, connectionSquidId);

        hdfsBefore();
        fileTypeEnum = FileType.valueOf(fileType.toUpperCase());
        fileLineSeparator.setFileType(fileTypeEnum.name());
        //excel(xls,xlsx)
        if (FileType.isExcel(fileTypeEnum)) {
            outRDD = ((CustomJavaSparkContext) jsc).hdfsExcelFile(hdfsUrl, paths, EncodingType.UTF8, fileLineSeparator);
        } else if (FileType.isWord(fileTypeEnum) || fileTypeEnum == FileType.PDF) {
            outRDD = ((CustomJavaSparkContext) jsc).hdfsWordPdfFile(hdfsUrl, paths, EncodingType.UTF8, fileLineSeparator);
        }
        //txt csv log xml
        else {
            List<JavaRDD<Map<Integer, DataCell>>> all = new ArrayList<>();
            JavaRDD<Map<Integer, DataCell>> tmp;
            for (String p : paths) {
                if(!validateHdfsFile(p)) {
                    continue;
                }
                tmp = textFile(jsc, p);
                if (tmp != null)
                    all.add(tmp);
            }
            outRDD = jsc.union(all.toArray(new JavaRDD[all.size()]));
        }

        return null;
    }

    private boolean validateHdfsFile(String srcPath) {
        if(StringUtils.isBlank(srcPath)) {
            return false;
        }
        Path path = new Path(srcPath);
        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            FileStatus status = fs.getFileStatus(path);
            if(status.getLen() == 0 && status.isFile()) {
                return false;
            }
        } catch (IOException e) {
            log.error("hdfs 文件验证失败", e);
            throw new EngineException(e);
        }

        return true;
    }

    private void hdfsBefore() {
        hdfsUrl = "hdfs://" + getIp();
        String[] _paths = new String[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String p = paths[i];
            if (!p.contains("hdfs://")) {
                _paths[i] = hdfsUrl + p;
            } else {
                _paths[i] = p;
            }
        }
        paths = _paths;
    }

    @Override
    protected void clean() {
        super.clean();
        this.getCurrentFlow().addCleaner(new Cleaner() {
            public void doSuccess() {
                deleteFile();
            }
        });
    }

    private void deleteFile() {
        if (fileLineSeparator != null && fileLineSeparator.getPostProcess() == PostProcess.DELETE) {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsUrl);
            try {
                FileSystem fs = FileSystem.get(conf);
                for (String p : paths) {
                    fs.delete(new Path(p), false);
                }
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private JavaRDD<Map<Integer, DataCell>> textFile(JavaSparkContext jsc, String path) {
        JavaRDD<Map<Integer, DataCell>> outRDD = null;
        jsc.hadoopConfiguration().set("textinputformat.record.codec", codec);
        jsc.hadoopConfiguration().set("textinputformat.record.filetype", String.valueOf(fileTypeEnum.name()));
        //xml
        if (fileTypeEnum == FileType.XML) {
            // 调用 hadoop api
            jsc.hadoopConfiguration().set("textinputformat.record.delimiter", fileLineSeparator.getXmlElementPath());
            JavaPairRDD<NullWritable, MapWritable> output =
                    jsc.hadoopFile(path, XmlStreamInputFormat.class, NullWritable.class, MapWritable.class);
            outRDD = output.map(new Function<Tuple2<NullWritable, MapWritable>,
                    Map<Integer, DataCell>>() {
                @Override
                public Map<Integer, DataCell> call(Tuple2<NullWritable, MapWritable> x) {
                    Map<Integer, DataCell> result = new HashMap<>();
                    MapWritable tmp = x._2();
                    for (TColumn col : fileLineSeparator.getColumns()) {
                        Object val = tmp.get(new Text(col.getName()));
                        if (val != null) {
                            result.put(col.getId(), new DataCell(col.getData_type(), TColumn.toTColumnValue(val.toString(), col)));
                        } else {
                            result.put(col.getId(), new DataCell(col.getData_type(), null));
                        }
                    }
                    return result;
                }
            });
        }
        //log csv => read line
        //txt => read record
        else {
            logBeforeStart(jsc);
            jsc.hadoopConfiguration().set("textinputformat.record.firstdatarowno", String.valueOf(fileLineSeparator.getFirstDataRowNo()));
            if (fileTypeEnum == FileType.TXT) {
                jsc.hadoopConfiguration().set("textinputformat.record.delimiter",
                        fileLineSeparator.getRowDelimiter());
            } else {
                jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "");
            }
            JavaRDD<String> javaRDD = jsc.sc()
                    .hadoopFile(path, com.eurlanda.datashire.engine.hadoop.TextInputFormat.class,
                            LongWritable.class, Text.class, jsc.sc().defaultMinPartitions())
                    .toJavaRDD()
                    .map(new Function<Tuple2<LongWritable, Text>, String>() {
                        @Override
                        public String call(Tuple2<LongWritable, Text> in) throws Exception {
                            return in._2().toString();
                        }
                    });
            outRDD = javaRDD.map(new Function<String, Map<Integer, DataCell>>() {
                @Override
                public Map<Integer, DataCell> call(String s) throws Exception {
                    return fileLineSeparator.separate(s);
                }
            }).filter(new Function<Map<Integer, DataCell>, Boolean>() {
                @Override
                public Boolean call(Map<Integer, DataCell> in) throws Exception {
                    return !in.isEmpty();
                }
            });
        }
        return outRDD;
    }

    private void logBeforeStart(JavaSparkContext jsc) {
        if (fileTypeEnum == FileType.TXT) {
            jsc.hadoopConfiguration().set("textinputformat.record.rowdelimiterposition", fileLineSeparator.getPosition().name());
            jsc.hadoopConfiguration().set("textinputformat.record.rowdelimiter", fileLineSeparator.getRowDelimiter());
        }
        if (fileTypeEnum == FileType.LOG) {
            switch (fileLineSeparator.getLogFormatType()) {
                case EXTENDED_LOG_FORMAT:
                    fileLineSeparator.setFirstDataRowNo(3);
                    break;
                default:
                    break;
            }
        }
    }

    public String[] getPaths() {
        return paths;
    }

    public void setPaths(String[] paths) {
        this.paths = paths;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public FileRecordSeparator getFileLineSeparator() {
        return fileLineSeparator;
    }

    public void setFileLineSeparator(FileRecordSeparator fileLineSeparator) {
        this.fileLineSeparator = fileLineSeparator;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getCodec() {
        return codec;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }
}

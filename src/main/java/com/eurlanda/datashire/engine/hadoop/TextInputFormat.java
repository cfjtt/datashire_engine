package com.eurlanda.datashire.engine.hadoop;

import com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by Juntao.Zhang on 2014/6/21.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextInputFormat extends FileInputFormat<LongWritable, Text>
        implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;
    private CompressionCodec codec;

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
        String codecName = conf.get("textinputformat.record.codec");
        if( codecName != null) {
            codec = compressionCodecs.getCodecByName(codecName);
        }
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
//        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }

    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        String firstDataRowNo = job.get("textinputformat.record.firstdatarowno");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter && !"".equals(delimiter)) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        RecordReader<LongWritable, Text> recordReader= new com.eurlanda.datashire.engine.hadoop.LineRecordReader(job, (FileSplit) genericSplit,
                recordDelimiterBytes,Long.valueOf(firstDataRowNo));
        return recordReader;
    }
}

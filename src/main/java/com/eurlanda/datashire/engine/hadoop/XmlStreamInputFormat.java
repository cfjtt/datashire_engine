package com.eurlanda.datashire.engine.hadoop;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;

import java.io.IOException;
import java.util.ArrayList;

/**
 * xml 格式化
 * Created by Juntao.Zhang on 2014/6/5.
 */
public class XmlStreamInputFormat extends FileInputFormat<NullWritable, MapWritable>
        implements JobConfigurable {

    public void configure(JobConf conf) {
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        FileStatus[] files = listStatus(job);

        // Save the number of input files for metrics/loadgen
        job.setLong(NUM_INPUT_FILES, files.length);

        // generate splits
        ArrayList<FileSplit> splits = new ArrayList<>(numSplits);
        NetworkTopology clusterMap = new NetworkTopology();
        for (FileStatus file : files) {
            Path path = file.getPath();
            long length = file.getLen();
            if (length != 0) {
                FileSystem fs = path.getFileSystem(job);
                BlockLocation[] blkLocations;
                if (file instanceof LocatedFileStatus) {
                    blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                } else {
                    blkLocations = fs.getFileBlockLocations(file, 0, length);
                }
                String[] splitHosts = getSplitHosts(blkLocations, 0, length, clusterMap);
                splits.add(makeSplit(path, 0, length, splitHosts));
            } else {
                //Create empty hosts array for zero length files
                splits.add(makeSplit(path, 0, length, new String[0]));
            }
        }
        LOG.debug("Total # of splits: " + splits.size());
        return splits.toArray(new FileSplit[splits.size()]);
    }

    public RecordReader<NullWritable, MapWritable> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        return new XmlRecordReader(job, (FileSplit) genericSplit, recordDelimiterBytes);
    }
}

package com.eurlanda.datashire.engine.hadoop;

import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition;
import com.eurlanda.datashire.enumeration.FileType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Unstable
public class LineRecordReader implements RecordReader<LongWritable, Text> {
    private static final Log log = LogFactory.getLog(LineRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;
    private long currentRowNo = -1;
    private long start;
    private long pos;
    private long end;
    private CustomRecordReader recordReader;
    private FSDataInputStream fileIn;
    private final Seekable filePosition;
    int maxLineLength;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private FileType fileType;
    private RowDelimiterPosition rowDelimiterPosition;
    private String rowDelimiter;

    public LineRecordReader(Configuration job, FileSplit split,
                            byte[] recordDelimiter,
                            long firstDataRowNo) throws IOException {
        this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
//                LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
                LineRecordReader.MAX_LINE_LENGTH, 100 * 1024 * 1024); // 100m
        setConfiguration(job);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        // 将压缩的编码交给用户指定
        String codecName = job.get("textinputformat.record.codec");
        if( codecName != null) {
            codec = compressionCodecs.getCodecByName(codecName);
        }
//        codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        if (isCompressedInput()) {
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream cIn =
                        ((SplittableCompressionCodec)codec).createInputStream(
                                fileIn, decompressor, start, end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                recordReader = new CustomCompressedSplitRecordReader(cIn, job, recordDelimiter);
                start = cIn.getAdjustedStart();
                end = cIn.getAdjustedEnd();
                filePosition = cIn; // take pos from compressed stream
            } else {
                recordReader = new CustomRecordReader(codec.createInputStream(fileIn, decompressor), job, recordDelimiter);
                filePosition = fileIn;
            }
        } else {
            fileIn.seek(start);
            recordReader = new CustomRecordReader(fileIn, job, recordDelimiter);
            filePosition = fileIn;
        }
        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            if (fileType == FileType.TXT) {
//                start += recordReader.readRecord(new Text(), 0, maxBytesToConsume(start));
                start += recordReader.readLine(new Text(), maxLineLength, maxBytesToConsume(start));
            } else {
                start += recordReader.readLine(new Text(), maxLineLength, maxBytesToConsume(start));
            }
        } else {
            //if first record we skip to firstDataRowNo
            while (currentRowNo < firstDataRowNo) {
                Text value = new Text();
                //此处不管哪种类型都是跳过行
                start += recordReader.readLine(value, maxLineLength, maxBytesToConsume(start));
                log.debug("skip line " + value);
                currentRowNo++;
            }
        }
        this.pos = start;
    }

    private void setConfiguration(Configuration job) {
        this.fileType = FileType.valueOf(job.get("textinputformat.record.filetype"));
        if (fileType == FileType.TXT) {
            rowDelimiter = job.get("textinputformat.record.rowdelimiter");
            rowDelimiterPosition = RowDelimiterPosition.valueOf(job.get("textinputformat.record.rowdelimiterposition"));
        }
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    private boolean isCompressedInput() {
        return (codec != null);
    }

    private int maxBytesToConsume(long pos) {
        return isCompressedInput()
                ? Integer.MAX_VALUE
                : (int) Math.min(Integer.MAX_VALUE, end - pos);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput() && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

    /** Read a line. */
    public synchronized boolean next(LongWritable key, Text value)
            throws IOException {

        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        while (getFilePosition() <= end || recordReader.needAdditionalRecordAfterSplit()) {
            key.set(pos);

            int newSize;
            if (fileType == FileType.TXT) {
//                newSize = recordReader.readRecord(value, maxLineLength,
//                        Math.max(maxBytesToConsume(pos), maxLineLength));
                newSize = recordReader.readLine(value, maxLineLength,
                        Math.max(maxBytesToConsume(pos), maxLineLength));
            } else {
                newSize = recordReader.readLine(value, maxLineLength,
                        Math.max(maxBytesToConsume(pos), maxLineLength));
            }
            if (newSize == 0) {
                return false;
            }
            // 去掉BOM
            if(pos == 0) {
                int textLength = value.getLength();
                byte[] textBytes = value.getBytes();
                if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
                        (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
                    // find UTF-8 BOM, strip it.
                    log.info("Found UTF-8 BOM and skipped it");
                    textLength -= 3;
//                    newSize -= 3;
                    if (textLength > 0) {
                        // It may work to use the same buffer and not do the copyBytes
                        textBytes = value.copyBytes();
                        value.set(textBytes, 3, textLength);
                    } else {
                        value.clear();
                    }
                }
            }

            pos += newSize;
            if (newSize < maxLineLength) {
                return true;
            }

            // line too long. try again
            log.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }

        return false;
    }

    /**
     * Get the progress within the split
     */
    public synchronized float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
        }
    }

    public  synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        try {
            if (recordReader != null) {
                recordReader.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }
}
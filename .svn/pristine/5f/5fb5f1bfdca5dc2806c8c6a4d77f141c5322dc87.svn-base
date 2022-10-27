package com.eurlanda.datashire.engine.hadoop;

/**
 * Created by Juntao.Zhang on 2014/6/6.
 */

import com.eurlanda.datashire.engine.entity.FileRecordSeparator;
import com.eurlanda.datashire.engine.exception.EngineException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.util.Map;

/**
 * Treats keys as offset in file and value as line.
 */
public class XmlRecordReader implements RecordReader<NullWritable, MapWritable> {
    private static final Log LOG = LogFactory.getLog(XmlRecordReader.class.getName());

    private long start;
    private long pos;
    private long end;
    private XMLEventReader in;
    private FSDataInputStream fileIn;
    int maxLineLength;
    FileRecordSeparator fileRecordSeparator;

    public XmlRecordReader(Configuration job, FileSplit split,byte[] recordDelimiter) throws IOException {
        this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
                LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
        //init FileLineSeparator 处理器
        fileRecordSeparator = new FileRecordSeparator();
        fileRecordSeparator.setXmlElementPath(new String(recordDelimiter));

        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        try {
            fileIn.seek(start);
            XMLInputFactory xif = XMLInputFactory.newInstance();
            in = xif.createXMLEventReader(fileIn);
        } catch (XMLStreamException e) {
            LOG.error(e.getMessage(), e);
        }
        this.pos = start;
    }

    public NullWritable createKey() {
        return NullWritable.get();
    }

    public MapWritable createValue() {
        return new MapWritable();
    }

    /**
     * Read a xml node.
     */
    public synchronized boolean next(NullWritable key, MapWritable value)
            throws IOException {
        try {
            if (!in.hasNext()) return false;
            XMLEvent xmlEvent = in.nextEvent();
            if (xmlEvent == null) return false;
            Map<String, String> infoMap = fileRecordSeparator.separateInternal(xmlEvent, in);
            if (MapUtils.isEmpty(infoMap)) return false;
            for (Map.Entry<String, String> entry : infoMap.entrySet()) {
                value.put(new Text(entry.getKey()), new Text(entry.getValue()));
            }
        } catch (XMLStreamException | EngineException e) {
            throw new RuntimeException("读取文件异常");
        }
        return true;
    }

    public synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        if (in != null) {
            try {
                in.close();
            } catch (XMLStreamException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    //todo juntao.zhang need show progress
    @Override
    public float getProgress() throws IOException {
        return 0.0f;
    }
}

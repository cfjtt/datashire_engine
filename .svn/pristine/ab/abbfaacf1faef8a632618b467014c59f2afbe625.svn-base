package com.eurlanda.datashire.engine.hadoop;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * A class that provides a line reader from an input stream.
 * Depending on the constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR),
 * or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated
 * line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class CustomCompressedSplitRecordReader extends CustomRecordReader {

    SplitCompressionInputStream scin;
    private boolean usingCRLF;
    private boolean needAdditionalRecord = false;
    private boolean finished = false;

    public CustomCompressedSplitRecordReader(SplitCompressionInputStream in,
                                            Configuration conf,
                                            byte[] recordDelimiterBytes) throws IOException {
        super(in, conf, recordDelimiterBytes);
        scin = in;
        usingCRLF = (recordDelimiterBytes == null);
    }

    @Override
    protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter)
            throws IOException {
        int bytesRead = in.read(buffer);

        // If the split ended in the middle of a record delimiter then we need
        // to read one additional record, as the consumer of the next split will
        // not recognize the partial delimiter as a record.
        // However if using the default delimiter and the next character is a
        // linefeed then next split will treat it as a delimiter all by itself
        // and the additional record read should not be performed.
        if (inDelimiter && bytesRead > 0) {
            if (usingCRLF) {
                needAdditionalRecord = (buffer[0] != '\n');
            } else {
                needAdditionalRecord = true;
            }
        }
        return bytesRead;
    }

    @Override
    public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
            throws IOException {
        int bytesRead = 0;
        if (!finished) {
            // only allow at most one more record to be read after the stream
            // reports the split ended
            if (scin.getPos() > scin.getAdjustedEnd()) {
                finished = true;
            }

            bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
        }
        return bytesRead;
    }

    @Override
    public boolean needAdditionalRecordAfterSplit() {
        return !finished && needAdditionalRecord;
    }
}


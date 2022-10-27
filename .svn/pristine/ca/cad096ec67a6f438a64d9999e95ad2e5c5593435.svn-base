package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.exception.OperationNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by zhudebin on 2017/3/10.
 */
public class CSVExtractor {

    private InputStream in;

    public CSVExtractor(InputStream in) {
        this.in = in;
    }

    public Iterator<List<String>> iterator() {

        final InputStream inputStream = in;

        return new Iterator<List<String>>() {

            Logger logger = LoggerFactory.getLogger(this.getClass());

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));

            boolean isEnd = false;

            String line = null;
            @Override
            public boolean hasNext() {

                if(line != null) {
                    return true;
                } else {
                    if(isEnd) {  // 判断文件是否已经读取完毕
                        return false;
                    }
                    try {
                        line = br.readLine();
                        if(line == null) {
                            isEnd = true;
                            return false;
                        }
                        return true;
                    } catch (IOException e) {
                        logger.error("读取文件异常");
                        close();
                        isEnd = true;
                        return false;
                    }
                }
            }

            @Override
            public List<String> next() {
                if(!this.hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    String _line = line;
                    line = null;    // 已经取,将中间缓存数据置空
                    return Arrays.asList(_line.split("\\t"));
                }
            }

            @Override
            public void remove() {
                throw new OperationNotSupportedException("该迭代器不支持remove");
            }

            private void close() {
                try {
                    br.close();
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        };

    }
}

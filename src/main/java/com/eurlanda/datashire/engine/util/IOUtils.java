package com.eurlanda.datashire.engine.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringBufferInputStream;

/**
 * Created by zhudebin on 14-5-27.
 */
public class IOUtils {

    public static String writeObjectToString(Object object) throws IOException {
        return new String(readObjectToBytes(object));
    }

    public static byte[] readObjectToBytes(Object object) throws IOException {
        // 获取模型二进制字符串
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(baos));
        oos.writeObject(object);
        oos.flush();
        return baos.toByteArray();
    }

    public static <T> T genObjectFromBytes(byte[] bytes, Class<T> tClass) throws IOException, ClassNotFoundException {
        return (T)(new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject());
    }

    public static <T> T readObjectFromString(String str, Class<T> tClass) throws IOException,ClassNotFoundException {
        return (T)(new ObjectInputStream(new StringBufferInputStream(str)).readObject());
    }
}

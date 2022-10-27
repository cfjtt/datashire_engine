package com.eurlanda.datashire.engine.java;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * Created by zhudebin on 16/5/26.
 */
public class FileTest {

    @Test
    public void testEncoding() {
        try {
            BufferedReader br =  new BufferedReader(new InputStreamReader(
                    new FileInputStream("/Users/zhudebin/111.csv"), "utf-8"));
            String line = null;
            int idx = 0;
            while((line = br.readLine()) != null) {
                System.out.println(line);
                if(idx ++ > 20) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

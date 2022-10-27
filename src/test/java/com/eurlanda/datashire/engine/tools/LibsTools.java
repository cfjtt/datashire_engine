package com.eurlanda.datashire.engine.tools;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;

/**
 * Created by zhudebin on 16/1/26.
 */
public class LibsTools {

    public static void main(String[] args) throws Exception {

        collectLibs();
    }

    public static void collectLibs() throws Exception {
        String targetDir = "/Users/zhudebin/Documents/iworkspace/datashire_dev3/out/artifacts/engine_libs/";
        String originDir = "/Users/zhudebin/Documents/iworkspace/datashire_dev3/out/artifacts/engine_jar2/";

        String list = "/Users/zhudebin/Documents/iworkspace/datashire_svn/datashire_engine/branches/branch_1.9_yarn/doc/libs2.txt";


        BufferedReader br = new BufferedReader(new FileReader(list));

        String line = null;

        while((line = br.readLine()) != null) {
            IOUtils.copy(new FileInputStream(originDir + line), new FileOutputStream(targetDir + line));
        }
    }

    public static void test1() throws Exception {
        FileInputStream fis = new FileInputStream("/Users/zhudebin/Documents/iworkspace/datashire_svn/datashire_engine/branches/branch_1.9_yarn/doc/libs.txt");

        InputStreamReader isr = new InputStreamReader(fis);

        BufferedReader br = new BufferedReader(isr);
        String str = br.readLine();

        BufferedWriter bw = new BufferedWriter(new FileWriter("/Users/zhudebin/Documents/iworkspace/datashire_svn/datashire_engine/branches/branch_1.9_yarn/doc/libs2.txt"));

        String[] strs = str.split(":");
        for(String s : strs) {

            String[] ss = s.split("/");
            //            System.out.println(ss[ss.length-1]);
            bw.write(ss[ss.length-1]);
            bw.newLine();
        }
        bw.flush();
        bw.close();
        br.close();
    }
}

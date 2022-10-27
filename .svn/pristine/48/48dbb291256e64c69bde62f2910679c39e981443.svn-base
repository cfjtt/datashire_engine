package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Created by zhudebin on 16/2/25.
 */
public class ScanUtil {

    public static String convertScanToString(Scan scan) throws IOException {
        return TableMapReduceUtil.convertScanToString(scan);
    }

    public static Scan convertStringToScan(String base64) throws IOException {
        return TableMapReduceUtil.convertStringToScan(base64);
    }
}

package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.FileRecordSeparator;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TFtpFileSquid;
import com.eurlanda.datashire.engine.entity.THdfsSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.enumeration.LogFormatType;
import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.EncodingType;
import org.apache.spark.CustomJavaSparkContext;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * Created by Juntao.Zhang on 2014/5/15.
 */
public class ApacheLogFileSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
//        TSquidFlow flow = createFtpSquidFlow("ftp_file_squid_apache_log", "LOG");
        TSquidFlow flow = createHdfsSquidFlow("hdfs_file_squid_apache_log", "LOG");
        flow.run(new CustomJavaSparkContext(getSparkMasterUrl(), "test", getSparkHomeDir(), getSparkJarLocation()));


    }

    public static TSquidFlow createFtpSquidFlow(String tableName, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TFtpFileSquid ftpFileSquid = createFtpFileSquid(fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(ftpFileSquid, tableName);

        squidFlow.addSquid(ftpFileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    public static TSquidFlow createHdfsSquidFlow(String tableName, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        THdfsSquid fileSquid = createHdfsFileSquid(fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(fileSquid, tableName);

        squidFlow.addSquid(fileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    private static TFtpFileSquid createFtpFileSquid(String fileType) {
        TFtpFileSquid ftpFileSquid = new TFtpFileSquid();
        ftpFileSquid.setId("1");
        ftpFileSquid.setSquidId(1);
        ftpFileSquid.setEncoding(EncodingType.UTF8);
        ftpFileSquid.setFileType(fileType);
        ftpFileSquid.setFiles(new String[]{"/spark/test4.log"});
        ftpFileSquid.setIp("192.168.137.114");
        ftpFileSquid.setUsername("ftpuser");
        ftpFileSquid.setPassword("ftpuser");
        ftpFileSquid.setPort(21);
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("user_identifier", 2, TDataType.STRING, true, true));
        columns.add(new TColumn("userid", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_datetime", 4, TDataType.TIMESTAMP, true));
        columns.add(new TColumn("request", 5, TDataType.STRING, true));
        columns.add(new TColumn("http_status_code", 6, TDataType.STRING, true));
        columns.add(new TColumn("resource_size", 7, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setColumns(columns);
        separator.setDateFormat(new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]", Locale.US));
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.COMMON_LOG_FORMAT);
        ftpFileSquid.setFileLineSeparator(separator);
        return ftpFileSquid;
    }

    private static THdfsSquid createHdfsFileSquid(String fileType) {
        THdfsSquid squid = new THdfsSquid();
        squid.setId("1");
        squid.setSquidId(1);
        squid.setFileType(fileType);
        squid.setPaths(new String[]{"hdfs://e201:9000/spark/data/test4.log", "hdfs://e201:9000/spark/data/test3.log"});
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("user_identifier", 2, TDataType.STRING, true, true));
        columns.add(new TColumn("userid", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_datetime", 4, TDataType.TIMESTAMP, true));
        columns.add(new TColumn("request", 5, TDataType.STRING, true));
        columns.add(new TColumn("http_status_code", 6, TDataType.STRING, true));
        columns.add(new TColumn("resource_size", 7, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setColumns(columns);
        separator.setDateFormat(new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]", Locale.US));
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.COMMON_LOG_FORMAT);
        squid.setFileLineSeparator(separator);
        return squid;
    }

    private static TDataFallSquid createDataFallSquid(TSquid preSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);
        dataFallSquid.setTruncateExistingData(true);

        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("user_identifier", 2, TDataType.STRING, true, true));
        columns.add(new TColumn("userid", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_datetime", 4, TDataType.TIMESTAMP, true, true));
        columns.add(new TColumn("request", 5, TDataType.STRING, true));
        columns.add(new TColumn("http_status_code", 6, TDataType.STRING, true));
        columns.add(new TColumn("resource_size", 7, TDataType.STRING, true));

        dataFallSquid.setColumnSet(columns);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }
}

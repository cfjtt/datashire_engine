package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.FileRecordSeparator;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TFtpFileSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.enumeration.LogFormatType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.EncodingType;
import org.apache.spark.CustomJavaSparkContext;

import java.util.HashSet;
import java.util.Set;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * Created by Juntao.Zhang on 2014/5/15.
 */
public class FtpExtendLogFileSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
        TSquidFlow flow = createSquidFlow("ftp_file_squid_ext_log", "LOG");
        flow.run(new CustomJavaSparkContext(getSparkMasterUrl(), "test", getSparkHomeDir(), getSparkJarLocation()));


    }

    public static TSquidFlow createSquidFlow(String tableName, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TFtpFileSquid ftpFileSquid = createFtpFileSquid(fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(ftpFileSquid, tableName);

        squidFlow.addSquid(ftpFileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    private static TFtpFileSquid createFtpFileSquid(String fileType) {
        TFtpFileSquid ftpFileSquid = new TFtpFileSquid();
        ftpFileSquid.setId("1");
        ftpFileSquid.setSquidId(1);
        ftpFileSquid.setEncoding(EncodingType.UTF8);
        ftpFileSquid.setFileType(fileType);
        ftpFileSquid.setFiles(new String[]{"/spark/u_ex140520.log","/spark/u_ex140522.log"});
        ftpFileSquid.setIp("192.168.137.114");
        ftpFileSquid.setUsername("ftpuser");
        ftpFileSquid.setPassword("ftpuser");
        ftpFileSquid.setPort(21);
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("date", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("time", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("c-ip", 4, TDataType.STRING, true, true));
        columns.add(new TColumn("cs-username", 5, TDataType.STRING, true));
        columns.add(new TColumn("s-ip", 6, TDataType.STRING, true));
        columns.add(new TColumn("s-port", 7, TDataType.STRING, true));
        columns.add(new TColumn("cs-method", 8, TDataType.STRING, true));
        columns.add(new TColumn("cs-uri-stem", 9, TDataType.STRING, true));
        columns.add(new TColumn("sc-status", 11, TDataType.STRING, true));
        columns.add(new TColumn("sc-win32-status", 12, TDataType.STRING, true));
        columns.add(new TColumn("sc-substatus", 13, TDataType.STRING, true));
        columns.add(new TColumn("x-session", 14, TDataType.STRING, true));
        columns.add(new TColumn("x-fullpath", 15, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setColumns(columns);
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.EXTENDED_LOG_FORMAT);
        ftpFileSquid.setFileLineSeparator(separator);
        return ftpFileSquid;
    }

    private static TDataFallSquid createDataFallSquid(TSquid preSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);
        dataFallSquid.setTruncateExistingData(true);

        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("date", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("time", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("c-ip", 4, TDataType.STRING, true, true));
        columns.add(new TColumn("cs-username", 5, TDataType.STRING, true));
        columns.add(new TColumn("s-ip", 6, TDataType.STRING, true));
        columns.add(new TColumn("s-port", 7, TDataType.STRING, true));
        columns.add(new TColumn("cs-method", 8, TDataType.STRING, true));
        columns.add(new TColumn("cs-uri-stem", 9, TDataType.STRING, true));
        columns.add(new TColumn("sc-status", 11, TDataType.STRING, true));
        columns.add(new TColumn("sc-win32-status", 12, TDataType.STRING, true));
        columns.add(new TColumn("sc-substatus", 13, TDataType.STRING, true));
        columns.add(new TColumn("x-session", 14, TDataType.STRING, true));
        columns.add(new TColumn("x-fullpath", 15, TDataType.STRING, true));

        dataFallSquid.setColumnSet(columns);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }
}

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
public class FtpIISLogFileSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
        TSquidFlow flow = createSquidFlow("ftp_file_squid_iis_log", "LOG");
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
        ftpFileSquid.setFiles(new String[]{"/spark/iis.log"});
        ftpFileSquid.setIp("192.168.137.114");
        ftpFileSquid.setUsername("ftpuser");
        ftpFileSquid.setPassword("ftpuser");
        ftpFileSquid.setPort(21);
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("client_ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("user_name", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_date", 4, TDataType.STRING, true, true));
        columns.add(new TColumn("request_time", 5, TDataType.STRING, true));
        columns.add(new TColumn("service_instance", 6, TDataType.STRING, true));
        columns.add(new TColumn("server_name", 7, TDataType.STRING, true));
        columns.add(new TColumn("server_ip", 8, TDataType.STRING, true));
        columns.add(new TColumn("time_taken", 9, TDataType.STRING, true));
        columns.add(new TColumn("client_bytes_sent", 11, TDataType.STRING, true));
        columns.add(new TColumn("server_bytes_sent", 12, TDataType.STRING, true));
        columns.add(new TColumn("service_status_code", 13, TDataType.STRING, true));
        columns.add(new TColumn("windows_status_code", 14, TDataType.STRING, true));
        columns.add(new TColumn("request_type", 10, TDataType.STRING, true));
        columns.add(new TColumn("operation_target", 0, TDataType.STRING, true));
        columns.add(new TColumn("parameters", 2, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setColumns(columns);
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.IIS_LOG_FILE_FORMAT);
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
        columns.add(new TColumn("client_ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("user_name", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_date", 4, TDataType.STRING, true, true));
        columns.add(new TColumn("request_time", 5, TDataType.STRING, true));
        columns.add(new TColumn("service_instance", 6, TDataType.STRING, true));
        columns.add(new TColumn("server_name", 7, TDataType.STRING, true));
        columns.add(new TColumn("server_ip", 8, TDataType.STRING, true));
        columns.add(new TColumn("time_taken", 9, TDataType.STRING, true));
        columns.add(new TColumn("client_bytes_sent", 11, TDataType.STRING, true));
        columns.add(new TColumn("server_bytes_sent", 12, TDataType.STRING, true));
        columns.add(new TColumn("service_status_code", 13, TDataType.STRING, true));
        columns.add(new TColumn("windows_status_code", 14, TDataType.STRING, true));
        columns.add(new TColumn("request_type", 10, TDataType.STRING, true));
        columns.add(new TColumn("operation_target", 0, TDataType.STRING, true));
        columns.add(new TColumn("parameters", 2, TDataType.STRING, true));

        dataFallSquid.setColumnSet(columns);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }
}

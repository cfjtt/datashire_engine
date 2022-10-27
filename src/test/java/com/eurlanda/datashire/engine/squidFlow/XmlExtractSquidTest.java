package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.EncodingType;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * xml 3种方式 extract test
 * Created by Juntao.Zhang on 2014/5/15.
 */
public class XmlExtractSquidTest extends AbstractSquidTest {
    //ftp
    @Test
    public void testFtpXmlFile() throws Exception {
        TSquidFlow flow = createFtpSquidFlow("ftp_file_squid_xml", "XML");
        flow.run(sc);
    }

    //共享文件夹
    @Test
    public void testShared() throws EngineException  {
        TSquidFlow flow = createSharedSquidFlow("shared_file_squid_xml", "XML");
        flow.run(sc);
    }

    //hdfs
    @Test
    public void testHdfs() throws EngineException  {
        TSquidFlow flow = createHdfsSquidFlow("hdfs_file_squid_xml", "XML");
        flow.run(sc);
    }

    public static TSquidFlow createHdfsSquidFlow(String tableName, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        THdfsSquid hdfsFileSquid = createHdfsFileSquid(fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(hdfsFileSquid, tableName);

        squidFlow.addSquid(hdfsFileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    public static TSquidFlow createFtpSquidFlow(String tableName, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TFtpFileSquid ftpFileSquid = createFtpFileSquid(fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(ftpFileSquid, tableName);

        squidFlow.addSquid(ftpFileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    public static TSquidFlow createSharedSquidFlow(String tableName, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TSharedFileSquid fileSquid = createRemoteFileSquid(fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(fileSquid, tableName);

        squidFlow.addSquid(fileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    private static TSharedFileSquid createRemoteFileSquid(String fileType) {
        TSharedFileSquid squid = new TSharedFileSquid();
        squid.setId("1");
        squid.setSquidId(1);
        squid.setEncoding(EncodingType.UTF8);
        squid.setFileType(fileType);
        squid.setFiles(new String[]{"datashire_engine_9/data/xml/test1.xml", "datashire_engine_9/data/xml/test2.xml"});
        squid.setIp("192.168.137.3");
        squid.setUsername("administrator");
        squid.setPassword("111111");
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("user_name", 2, TDataType.STRING, true));
        columns.add(new TColumn("user_id", 1, TDataType.LONG, true, true));
        columns.add(new TColumn("company_name", 3, TDataType.STRING, true));
        squid.setFileLineSeparator(new FileRecordSeparator(columns, "persons/person"));
        return squid;
    }

    private static THdfsSquid createHdfsFileSquid(String fileType) {
        THdfsSquid squid = new THdfsSquid();
        squid.setId("1");
        squid.setSquidId(1);
        squid.setFileType(fileType);
        squid.setPaths(new String[]{"hdfs://e201:9000/spark/data/file/xml/test1.xml", "hdfs://e201:9000/spark/data/test5.xml", "hdfs://e201:9000/spark/data/file/xml/test2.xml"});
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("user_name", 2, TDataType.STRING, true));
        columns.add(new TColumn("user_id", 1, TDataType.LONG, true, true));
        columns.add(new TColumn("company_name", 3, TDataType.STRING, true));
        squid.setFileLineSeparator(new FileRecordSeparator(columns, "persons/person"));
        return squid;
    }

    private static TFtpFileSquid createFtpFileSquid(String fileType) {
        TFtpFileSquid ftpFileSquid = new TFtpFileSquid();
        ftpFileSquid.setId("1");
        ftpFileSquid.setSquidId(1);
        ftpFileSquid.setEncoding(EncodingType.UTF8);
        ftpFileSquid.setFileType(fileType);
        ftpFileSquid.setFiles(new String[]{"/spark/xml/test1.xml", "/spark/xml/test2.xml"});
        ftpFileSquid.setIp("192.168.137.114");
        ftpFileSquid.setUsername("ftpuser");
        ftpFileSquid.setPassword("ftpuser");
        ftpFileSquid.setPort(21);
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("user_name", 2, TDataType.STRING, true));
        columns.add(new TColumn("user_id", 1, TDataType.LONG, true, true));
        columns.add(new TColumn("company_name", 3, TDataType.STRING, true));
        ftpFileSquid.setFileLineSeparator(new FileRecordSeparator(columns, "persons/person"));
        return ftpFileSquid;
    }

    private static TDataFallSquid createDataFallSquid(TSquid preSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);
        dataFallSquid.setTruncateExistingData(true);

        Set<TColumn> columnSet = new HashSet<>();
        columnSet.add(new TColumn("user_id", 1, TDataType.LONG, true, true));
        columnSet.add(new TColumn("user_name", 2, TDataType.STRING, true));
        columnSet.add(new TColumn("company_name", 3, TDataType.STRING, true));

        dataFallSquid.setColumnSet(columnSet);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }
}

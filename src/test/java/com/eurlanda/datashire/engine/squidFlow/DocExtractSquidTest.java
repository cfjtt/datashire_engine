package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.EncodingType;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;


/**
 * Created by Juntao.Zhang on 2014/5/15.
 */
public class DocExtractSquidTest extends AbstractSquidTest {

    //ftp
    @Test
    public void testFtpFileSquidPdf() throws Exception {
        TSquidFlow flow = createFtpSquidFlow("ftp_file_squid_pdf", "pdf", "PDF");
        flow.run(sc);
    }

    @Test
    public void testFtpFileSquidDelete() throws Exception {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TFtpFileSquid ftpFileSquid = new TFtpFileSquid();
        ftpFileSquid.setId("1");
        ftpFileSquid.setSquidId(1);
        ftpFileSquid.setEncoding(EncodingType.UTF8);
        ftpFileSquid.setFileType("TXT");
        ftpFileSquid.setFiles(new String[]{"/spark/txt/delete/test1.txt"});
        ftpFileSquid.setIp("192.168.137.114");
        ftpFileSquid.setUsername("ftpuser");
        ftpFileSquid.setPassword("ftpuser");
        ftpFileSquid.setPort(21);
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 12, TDataType.STRING, true));
        columns.add(new TColumn("0", 11, TDataType.LONG, true, true));
        columns.add(new TColumn("2", 13, TDataType.STRING, true));
        ftpFileSquid.setFileLineSeparator(new FileRecordSeparator(0, ",", 0, 1, columns));
        ftpFileSquid.getFileLineSeparator().setFileType("TXT");
        ftpFileSquid.getFileLineSeparator().setPosition(RowDelimiterPosition.End);
        ftpFileSquid.getFileLineSeparator().setRowDelimiter("\\r\\n");
        ftpFileSquid.getFileLineSeparator().setPostProcess(PostProcess.DELETE);
        TDataFallSquid dataFallSquid = createDataFallSquid(ftpFileSquid, "ftp_file_squid_txt");
        squidFlow.addSquid(ftpFileSquid);
        squidFlow.addSquid(dataFallSquid);
        squidFlow.run(sc);
    }

    @Test
    public void testFtpFileSquidTxt() throws Exception {
        TSquidFlow flow = createFtpSquidFlow("ftp_file_squid_txt", "txt", "TXT");
        flow.run(sc);
    }

    @Test
    public void testFtpFileSquidOffice() throws Exception {
        TSquidFlow flow = createFtpSquidFlow("ftp_file_squid_docx", "docx", "DOCX");
        flow.run(sc);
    }

    @Test
    public void testFtpFileSquidCSV() throws Exception {
        TSquidFlow flow = createFtpSquidFlow("ftp_file_squid_csv", "csv", "CSV");
        flow.run(sc);
    }

    //hdfs
    @Test
    public void testHdfsFileSquidDelete() throws Exception {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        THdfsSquid squid = new THdfsSquid();
        squid.setId("1");
        squid.setSquidId(1);
        squid.setFileType("TXT");
        squid.setIp("e231:9000");
//        squid.setPort("9000");
        squid.setPaths(new String[]{"/spark/data/txt/delete/test1.txt"});
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 12, TDataType.STRING, true));
        columns.add(new TColumn("0", 11, TDataType.STRING, true, true));
        columns.add(new TColumn("2", 13, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator(0, ",", 0, 1, columns);
        separator.setPosition(RowDelimiterPosition.End);
        separator.setRowDelimiter("\\r\\n");
        squid.setFileLineSeparator(separator);
        squid.getFileLineSeparator().setPostProcess(PostProcess.DELETE);
        TDataFallSquid dataFallSquid = createDataFallSquid(squid, "hdfs_file_squid_txt");
        squidFlow.addSquid(squid);
        squidFlow.addSquid(dataFallSquid);
        squidFlow.run(sc);
    }

    @Test
    public void testHdfsFileSquidPdf() throws Exception {
        TSquidFlow flow = createHdfsSquidFlow("hdfs_file_squid_pdf", "pdf", "PDF");
        flow.run(sc);
    }

    @Test
    public void testHdfsFileSquidTxt() throws Exception {
        TSquidFlow flow = createHdfsSquidFlow("hdfs_file_squid_txt", "txt", "TXT");
        flow.run(sc);
    }

    @Test
    public void testHdfsFileSquidOffice() throws Exception {
        TSquidFlow flow = createHdfsSquidFlow("hdfs_file_squid_docx", "docx", "DOCX");
        flow.run(sc);
    }

    @Test
    public void testHdfsFileSquidCSV() throws Exception {
        TSquidFlow flow = createHdfsSquidFlow("hdfs_file_squid_csv", "csv", "CSV");
        flow.run(sc);
    }

    @Test
    public void testHdfsFileSquidXLS() throws Exception {
        TSquidFlow flow = createHdfsSquidFlow("hdfs_file_squid_xls", "xls", "XLS");
        flow.run(sc);
    }

    //共享文件夹
    @Test
    public void testRemoteFileSquidDelete() throws Exception {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TSharedFileSquid squid = new TSharedFileSquid();
        squid.setId("1");
        squid.setSquidId(1);
        squid.setEncoding(EncodingType.UTF8);
        squid.setFileType("TXT");
        squid.setFiles(new String[]{"datashire_engine_9/data/txt/delete/test1.txt"});
        squid.setIp("192.168.137.3");
        squid.setUsername("administrator");
        squid.setPassword("111111");
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 12, TDataType.STRING, true));
        columns.add(new TColumn("0", 11, TDataType.LONG, true, true));
        columns.add(new TColumn("2", 13, TDataType.STRING, true));
        squid.setFileLineSeparator(new FileRecordSeparator(0, ",", 0, 1, columns));
        squid.getFileLineSeparator().setPosition(RowDelimiterPosition.End);
        squid.getFileLineSeparator().setRowDelimiter("\\r\\n");
        squid.getFileLineSeparator().setPostProcess(PostProcess.DELETE);
        TDataFallSquid dataFallSquid = createDataFallSquid(squid, "remote_file_squid_txt");
        squidFlow.addSquid(squid);
        squidFlow.addSquid(dataFallSquid);
        squidFlow.run(sc);
    }

    @Test
    public void testRemoteFileSquidPdf() throws Exception {
        TSquidFlow flow = createRemoteSquidFlow("remote_file_squid_pdf", "pdf", "PDF");
        flow.run(sc);
    }

    @Test
    public void testRemoteFileSquidXLS() throws Exception {
        TSquidFlow flow = createRemoteSquidFlow("remote_file_squid_xls", "xls", "XLS");
        flow.run(sc);
    }

    @Test
    public void testRemoteFileSquidTxt() throws Exception {
        TSquidFlow flow = createRemoteSquidFlow("remote_file_squid_txt", "txt", "TXT");
        flow.run(sc);
    }

    @Test
    public void testRemoteFileSquidOffice() throws Exception {
        TSquidFlow flow = createRemoteSquidFlow("remote_file_squid_docx", "docx", "DOCX");
        flow.run(sc);
    }

    @Test
    public void testRemoteFileSquidCSV() throws Exception {
        TSquidFlow flow = createRemoteSquidFlow("remote_file_squid_csv", "csv", "CSV");
        flow.run(sc);
    }

    private static TSquidFlow createFtpSquidFlow(String tableName, String suffix, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TFtpFileSquid ftpFileSquid = createFtpFileSquid(suffix, fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(ftpFileSquid, tableName);

        squidFlow.addSquid(ftpFileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    private static TSquidFlow createHdfsSquidFlow(String tableName, String suffix, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        THdfsSquid fileSquid = createHdfsFileSquid(suffix, fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(fileSquid, tableName);

        squidFlow.addSquid(fileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

    private static TSquidFlow createRemoteSquidFlow(String tableName, String suffix, String fileType) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TSharedFileSquid fileSquid = createRemoteFileSquid(suffix, fileType);
        TDataFallSquid dataFallSquid = createDataFallSquid(fileSquid, tableName);

        squidFlow.addSquid(fileSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }

//    private void check(TDataSource dataSource) throws Exception {
//        DriverManager.getConnection(dataSource.get, dbi.getUserName, dbi.getPassword);
//        Connection conn = getConnection(dataSource);
//        conn.setAutoCommit(false);
//        conn.prepareStatement(sql).executeUpdate();
//        conn.commit();
//        if (conn != null && !conn.isClosed()) {
//            conn.close();
//        }
//    }

    private static TSharedFileSquid createRemoteFileSquid(String suffix, String fileType) {
        TSharedFileSquid squid = new TSharedFileSquid();
        squid.setId("1");
        squid.setSquidId(1);
        squid.setEncoding(EncodingType.UTF8);
        squid.setFileType(fileType);
        squid.setFiles(new String[]{"datashire_engine_9/data/" + suffix + "/test1." + suffix, "datashire_engine_9/data/" + suffix + "/test2." + suffix});
        squid.setIp("192.168.137.3");
        squid.setUsername("administrator");
        squid.setPassword("admin");
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 12, TDataType.STRING, true));
        columns.add(new TColumn("0", 11, TDataType.STRING, true, true));
        columns.add(new TColumn("2", 13, TDataType.STRING, true));
        squid.setFileLineSeparator(new FileRecordSeparator(0, ",", 0, 1, columns));
        squid.getFileLineSeparator().setPosition(RowDelimiterPosition.End);
        squid.getFileLineSeparator().setRowDelimiter("\\r\\n");
        if (fileType.equals("DOCX")) {
            squid.getFileLineSeparator().setRowDelimiter("\\n");
        }
        return squid;
    }

    private static THdfsSquid createHdfsFileSquid(String suffix, String fileType) {
        THdfsSquid squid = new THdfsSquid();
        squid.setId("1");
        squid.setSquidId(1);
        squid.setFileType(fileType);
//        String path = "hdfs://" + ConfigurationUtil.getHdfsAddress() + "/spark/data/" + suffix;
        String path = "/user/hans";
        squid.setIp("e231:9000");
//        squid.setPort("9000");
//        squid.setPaths(new String[]{path + "/test4." + suffix});
        squid.setPaths(new String[]{path + "/test2." + suffix});
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 12, TDataType.STRING, true));
        columns.add(new TColumn("0", 11, TDataType.STRING, true, true));
        columns.add(new TColumn("2", 13, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator(0, ",", 0, 1, columns);
        separator.setPosition(RowDelimiterPosition.End);
        separator.setRowDelimiter("\\r\\n");
//        if (fileType.equals("DOCX")) {
//            separator.setRowDelimiter("\\n");
//        }
//        separator.setRowDelimiter(";");
        squid.setFileLineSeparator(separator);
        return squid;
    }

    private static TFtpFileSquid createFtpFileSquid(String suffix, String fileType) {
        TFtpFileSquid ftpFileSquid = new TFtpFileSquid();
        ftpFileSquid.setId("1");
        ftpFileSquid.setSquidId(1);
        ftpFileSquid.setEncoding(EncodingType.UTF8);
        ftpFileSquid.setFileType(fileType);
        ftpFileSquid.setFiles(new String[]{"/spark/" + suffix + "/test1." + suffix, "/spark/" + suffix + "/test2." + suffix});
        ftpFileSquid.setIp("192.168.137.114");
        ftpFileSquid.setUsername("ftpuser");
        ftpFileSquid.setPassword("ftpuser");
        ftpFileSquid.setPort(21);
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 12, TDataType.STRING, true));
        columns.add(new TColumn("0", 11, TDataType.LONG, true, true));
        columns.add(new TColumn("2", 13, TDataType.STRING, true));
        ftpFileSquid.setFileLineSeparator(new FileRecordSeparator(0, ",", 0, 1, columns));
        ftpFileSquid.getFileLineSeparator().setFileType(fileType);
        ftpFileSquid.getFileLineSeparator().setPosition(RowDelimiterPosition.End);
        ftpFileSquid.getFileLineSeparator().setRowDelimiter("\\r\\n");
        if (fileType.equals("DOCX")) {
            ftpFileSquid.getFileLineSeparator().setRowDelimiter("\\n");
        }
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
        columnSet.add(new TColumn("USER_ID", 11, TDataType.STRING, true, true));
        columnSet.add(new TColumn("USER_NAME", 12, TDataType.STRING, true));
        columnSet.add(new TColumn("COMPANY_NAME", 13, TDataType.STRING, true));

        dataFallSquid.setColumnSet(columnSet);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }

//    public static void main(String[] args) {
//    }
}

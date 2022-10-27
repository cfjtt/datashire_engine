package com.eurlanda.datashire.engine.entity;

import cn.com.jsoft.jframe.utils.fileParsers.OfficeFileParser;
import com.eurlanda.datashire.adapter.FileAdapter;
import com.eurlanda.datashire.engine.enumeration.LogFormatType;
import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition;
import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.utility.ExcelExtractor;
import com.eurlanda.datashire.utility.XSSFExcelExtractor;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.poi.POITextExtractor;
import org.apache.poi.extractor.ExtractorFactory;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.junit.Test;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 文件解析单元测试
 * Created by Juntao.Zhang on 2014/5/15.
 */
public class FileRecordSeparatorTest {
    @Test
    public void testTxtFileDelimiter() {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("2", 2, TDataType.STRING, true));
        columns.add(new TColumn("1", 1, TDataType.LONG, true, true));
        columns.add(new TColumn("3", 3, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator(0, ",", 0, 0, columns);
        separator.setFileType("TXT");
        Map<Integer, DataCell> result = separator.separate("1,Anne,Alulux-Office China");
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("Alulux-Office China", result.get(3).getData());
    }

    @Test
    public void testTxtFileFieldLength() {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 1, TDataType.LONG, true, true));
        columns.add(new TColumn("2", 2, TDataType.STRING, true));
        columns.add(new TColumn("3", 3, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator(1, null, 2, 0, columns);
        separator.setFileType("TXT");
        Map<Integer, DataCell> result = separator.separate("01SDHF");
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("SD", result.get(2).getData());
    }

    @Test
    public void testTxtFileWhole() {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("1", 1, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator(2, null, 0, 0, columns);
        Map<Integer, DataCell> result = separator.separate("Zhangjuntao");
        separator.setFileType("TXT");
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("Zhangjuntao", result.get(1).getData());
    }

    @Test
    public void testBigText() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(new File("data\\zjt\\test.dat")));
//        BufferedReader br = new BufferedReader(new FileReader(new File("G:\\document\\data\\t_lucene_content.sql")));
//        BufferedReader br = new BufferedReader(new FileReader(new File("G:\\DOWNLOAD\\CentOS-6.5-x86_64-bin-DVD1to2\\CentOS-6.5-x86_64-bin-DVD1.iso")));
        String s;
        int i = 50;
        while ((s = br.readLine()) != null && i-- > 0) {
            System.out.println(s);
        }
    }


    @Test
    public void testWord() throws Exception {
        BufferedReader br = new BufferedReader(new StringReader(new OfficeFileParser().getStringContent(new File("data/ftp/docx/test1.docx"))));
        String s;
        while ((s = br.readLine()) != null) {
            System.out.println(s);
        }
    }

    int count = 0;
    String record = "";

    @Test
    public void testTxt() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(new File("data/ftp/txt/test1.txt")));
        int firstDataRowNo = 5;
        int currentLine = 0;
        RowDelimiterPosition position = RowDelimiterPosition.End;
        LinkedList<String> list = new LinkedList<>();
        String reg = ";";//[\\r\\n]+
        skipToDirstDataRow(br, firstDataRowNo, currentLine);
        while (true) {
            boolean success = generateRecord(br, position, list, reg, 100, 2);
            while (!list.isEmpty()) {
                System.out.println("[" + list.getFirst() + "]");
                list.removeFirst();
            }
            if (!success) {
                break;
            }
        }
    }

    //跳到数据行
    private void skipToDirstDataRow(BufferedReader br, int firstDataRowNo, int currentLine) throws IOException {
        String s;
        while (currentLine++ < firstDataRowNo && (s = br.readLine()) != null) {
            System.out.println(s);
        }
        count = 0;
    }

    //获得doc多行一个record 或者 一行多个record方法
    private boolean generateRecord(BufferedReader br,
                                   RowDelimiterPosition position,
                                   List<String> list,
                                   String reg,
                                   int bufferSize,
                                   int recordSize) throws IOException {
        Pattern rowDelimiter = Pattern.compile(reg);
        char[] buf = new char[bufferSize];
        while (br.read(buf) != -1) {
            //最后一行去掉char[]中的0
            record += String.valueOf(buf).replace("\u0000", "");
            boolean flag = false;
            int start = 0;
            Matcher m = rowDelimiter.matcher(record);
            while (m.find()) {
                String tmp = record.substring(start, m.start());
                //如果是record以reg开始，跳过获得的第一条记录
                if (org.apache.commons.lang.StringUtils.isNotEmpty(tmp)
                        && ((RowDelimiterPosition.Begin == position && count != 0) || RowDelimiterPosition.End == position))
                    list.add(tmp);
                start = m.end();
                flag = true;
                count++;
            }
            if (flag) record = record.substring(start, record.length());
            buf = new char[bufferSize];
            if (list.size() >= recordSize) {
                return true;
            }
        }
        //如果是record以reg开始，处理最后一行问题
        if (org.apache.commons.lang.StringUtils.isNotBlank(record)
                && (RowDelimiterPosition.Begin == position && count != 0))
            list.add(record);
        return list.size() != 0;
    }

    @Test
    public void testXls() throws Exception {
//        InputStream inp = new FileInputStream("data/ftp/xls/test2.xls");
//        HSSFWorkbook wb = new HSSFWorkbook(new POIFSFileSystem(inp));
//        ExcelExtractor extractor = new ExcelExtractor(wb);
//
//        extractor.setFormulasNotResults(true);
//        extractor.setIncludeSheetNames(false);
//        String text = extractor.getText();
//        System.out.println(text);

//        POITextExtractor extractor = ExtractorFactory.createExtractor(new File("data/ftp/xls/test1.xls"));
        ExcelExtractor extractor = new ExcelExtractor(new POIFSFileSystem(new FileInputStream(new File("data/extract/xls/test1.xls"))));
        extractor.setIncludeBlankCells(true);
        extractor.setIncludeSheetNames(false);
        extractor.getRowListCellList();
        List<List<String>> lists = extractor.getRowListCellList();
        for (List<String> list : lists) {
            System.out.println(Arrays.toString(list.toArray()));
        }
    }
    @Test
    public void testXlsx() throws Exception {
//        POITextExtractor extractor = ExtractorFactory.createExtractor(new File("data/extract/xlsx/test1.xlsx"));
//        ((org.apache.poi.xssf.extractor.XSSFExcelExtractor)extractor).setIncludeSheetNames(false);

        XSSFExcelExtractor extractor = new XSSFExcelExtractor("data/extract/xlsx/test1.xlsx");
//        XSSFExcelExtractor extractor = new XSSFExcelExtractor("data/test_data/xlsx/blog/blog-gender-dataset.xlsx");
        extractor.setIncludeBlankCells(true);
        extractor.setIncludeCellComments(false);
        extractor.setIncludeSheetNames(false);
        List<List<String>> lists = extractor.getRowListCellList();
        for (List<String> list : lists) {
            System.out.println(Arrays.toString(list.toArray()));
        }
    }

//    public static void readFile(String filePath) throws IOException {
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
//        Path srcPath = new Path(filePath);
//        InputStream in = null;
//        try {
//            in = fs.open(srcPath);
//            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
//        } finally {
//            IOUtils.closeStream(in);
//        }
//    }

    @Test
    public void testDoc() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://e221:9000/test_data/doc_imdb.doc"), conf);
        Path srcPath = new Path("hdfs://e221:9000/test_data/doc_imdb.doc");
        InputStream in = null;
        in = fs.open(srcPath);
//        POITextExtractor extractor = ExtractorFactory.createExtractor(new File("data/extract/doc_imdb.doc"));
        POITextExtractor extractor = ExtractorFactory.createExtractor(in);
        BufferedReader br = new BufferedReader(new StringReader(extractor.getText()));

        char[] buf = new char[20];
        String record = "";
        List<String> list = new ArrayList<>();
        Pattern rowDelimiter = Pattern.compile("[\\r\\n]+");
        while (br.read(buf) != -1) {
            record += String.valueOf(buf);
            System.out.println(record);
            boolean flag = false;
            Matcher m = rowDelimiter.matcher(record);
            int start = 0;
            while (m.find()) {
                String tmp = record.substring(start, m.start());
                if (org.apache.commons.lang.StringUtils.isNotBlank(tmp))
                    list.add(tmp);
                start = m.end();
                flag = true;
            }
            if (flag) record = record.substring(start, record.length());
            buf = new char[20];
        }
        System.out.println(list.size());
    }

    @Test
    public void testPDf() throws Exception {
        BufferedReader br = new BufferedReader(new StringReader(new FileAdapter().getTextFromPdf(new File("data/ftp/test1.pdf"))));
        String s;
        while ((s = br.readLine()) != null) {
            System.out.println(s);
        }
//        PDDocument doc = PDDocument.load("data/ftp/test1.pdf");
//        PDFTextStripper stripper = new PDFTextStripper();
//        String text = stripper.getText(doc);
//        System.out.println(text);
    }

    @Test
    public void testXml() throws Exception {
        FileRecordSeparator fileLineSeparator = new FileRecordSeparator();
        Set<TColumn> columns = new HashSet<>();
        fileLineSeparator.setXmlElementPath("/catalog/book/a");
        columns.add(new TColumn(TDataType.STRING, "sku", 1));
        columns.add(new TColumn(TDataType.STRING, "title", 2));
        columns.add(new TColumn(TDataType.STRING, "author", 3));
        columns.add(new TColumn(TDataType.DOUBLE, "price", 4));
        columns.add(new TColumn(TDataType.STRING, "category", 5));
        fileLineSeparator.setColumns(columns);
        XMLInputFactory xif = XMLInputFactory.newInstance();
        try {
            // create the reader from the stream
            XMLEventReader reader = xif.createXMLEventReader(new FileReader("data\\ftp\\books.xml"));

//            XMLEventReader reader = xif.createXMLEventReader(new FileReader("G:\\document\\data\\test3.xml"));

            // work with stream and get the type of event
            // we're inspecting
            int i = 0;
            while (reader.hasNext()) {
                System.out.println("number:" + i++);
                XMLEvent xmlEvent = reader.nextEvent();
                System.out.println(fileLineSeparator.separate(xmlEvent, reader));
            } // end loop

        } catch (XMLStreamException e) {
            System.err.println("Cannot parse: " + e);
        }
    }


    @Test
    public void testXmlUsers() throws Exception {
        FileRecordSeparator fileLineSeparator = new FileRecordSeparator();
        Set<TColumn> columns = new HashSet<>();
        fileLineSeparator.setXmlElementPath("company/depart/user");
        columns.add(new TColumn(TDataType.STRING, "name", 1));
        columns.add(new TColumn(TDataType.STRING, "age", 2));
        columns.add(new TColumn(TDataType.STRING, "gender", 3));

//        fileLineSeparator.setBreakElement("depart");
//        columns.add(new TColumn(TDataType.STRING, "title", 1));

        fileLineSeparator.setColumns(columns);
        XMLInputFactory xif = XMLInputFactory.newInstance();
        try {
            // create the reader from the stream
            XMLEventReader reader = xif.createXMLEventReader(new FileReader("data\\ftp\\users.xml"));
            // work with stream and get the type of event
            // we're inspecting
            int i = 0;
            while (reader.hasNext()) {
                System.out.println("number:" + i++);
                XMLEvent xmlEvent = reader.nextEvent();
                System.out.println(fileLineSeparator.separate(xmlEvent, reader));
            } // end loop

        } catch (XMLStreamException e) {
            System.err.println("Cannot parse: " + e);
        }
    }

    @Test
    public void testXmlDeparts() throws Exception {
        FileRecordSeparator fileLineSeparator = new FileRecordSeparator();
        Set<TColumn> columns = new HashSet<>();
        fileLineSeparator.setXmlElementPath("company/depart");
        columns.add(new TColumn(TDataType.STRING, "title", 1));

        fileLineSeparator.setColumns(columns);
        XMLInputFactory xif = XMLInputFactory.newInstance();
        try {
            // create the reader from the stream
            XMLEventReader reader = xif.createXMLEventReader(new FileReader("data\\ftp\\users.xml"));
            // work with stream and get the type of event
            // we're inspecting
            int i = 0;
            while (reader.hasNext()) {
                System.out.println("number:" + i++);
                XMLEvent xmlEvent = reader.nextEvent();
                System.out.println(fileLineSeparator.separate(xmlEvent, reader));
            } // end loop

        } catch (XMLStreamException e) {
            System.err.println("Cannot parse: " + e);
        }
    }

    @Test
    public void testXmlTest() throws Exception {

        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("user_name", 2, TDataType.STRING, true));
        columns.add(new TColumn("user_id", 1, TDataType.LONG, true, true));
        columns.add(new TColumn("company_name", 3, TDataType.STRING, true));
        FileRecordSeparator fileLineSeparator = new FileRecordSeparator(columns, "persons/person");

        fileLineSeparator.setColumns(columns);
        XMLInputFactory xif = XMLInputFactory.newInstance();
        try {
            // create the reader from the stream
            XMLEventReader reader = xif.createXMLEventReader(new FileReader("data\\ftp\\test1.xml"));
            // work with stream and get the type of event
            // we're inspecting
            int i = 0;
            while (reader.hasNext()) {
                System.out.println("number:" + i++);
                XMLEvent xmlEvent = reader.nextEvent();
                System.out.println(fileLineSeparator.separate(xmlEvent, reader));
            } // end loop

        } catch (XMLStreamException e) {
            System.err.println("Cannot parse: " + e);
        }
    }


    @Test
    public void testApacheLog1() {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("userid", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_datetime", 4, TDataType.TIMESTAMP, true, true));
        columns.add(new TColumn("request", 5, TDataType.STRING, true));
        columns.add(new TColumn("http_status_code", 6, TDataType.STRING, true));
        columns.add(new TColumn("resource_size", 7, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setColumns(columns);
        separator.setDateFormat(new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]", Locale.US));
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.COMMON_LOG_FORMAT);
        Map<Integer, DataCell> result = separator.separate("127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200  2326");
        System.out.println(result);
    }

    @Test
    public void testApacheLogFile() throws Exception {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("user_identifier", 2, TDataType.STRING, true, true));
        columns.add(new TColumn("userid", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_datetime", 4, TDataType.TIMESTAMP, true, true));
        columns.add(new TColumn("request", 5, TDataType.STRING, true));
        columns.add(new TColumn("http_status_code", 6, TDataType.STRING, true));
        columns.add(new TColumn("resource_size", 7, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setColumns(columns);
        separator.setDateFormat(new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]", Locale.US));
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.COMMON_LOG_FORMAT);
        BufferedReader reader = new BufferedReader(new FileReader(new File("data\\ftp\\test4.log")));
        String line;
        List<Map<Integer, DataCell>> all = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            Map<Integer, DataCell> result = separator.separate(line);
            all.add(result);
        }
        System.out.println(all.size());
        //a213-84-36-192.adsl.xs4all.nl - - [08/Mar/2004:23:42:55 -0800] "GET / HTTP/1.1" 200 3169
//        Assert.assertEquals("a213-84-36-192.adsl.xs4all.nl",all.get(754).get(1).getData());
    }

    @Test
    public void testApacheLog2() {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("ip", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("userid", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("request_datetime", 4, TDataType.TIMESTAMP, true, true));
        columns.add(new TColumn("request", 5, TDataType.STRING, true));
        columns.add(new TColumn("http_status_code", 6, TDataType.LONG, true));
        columns.add(new TColumn("resource_size", 7, TDataType.LONG, true));
        columns.add(new TColumn("referer", 8, TDataType.STRING, true));
        columns.add(new TColumn("user_agent", 9, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setColumns(columns);
        separator.setDateFormat(new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]", Locale.US));
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.APACHE_COMBINED_FORMAT);
        Map<Integer, DataCell> result = separator.separate("127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" " +
                "200  2326  \"http://www.example.com/start.html\" " +
                "\"Mozilla/4.08 [en] (Win98; I ;Nav)\"");

        Assert.assertEquals("\"Mozilla/4.08 [en] (Win98; I ;Nav)\"", result.get(9).getData());
    }

    @Test
    public void testExtendedLog() {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("date", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("time", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("c-ip", 4, TDataType.STRING, true, true));
        columns.add(new TColumn("cs-username", 5, TDataType.STRING, true));
        columns.add(new TColumn("s-ip", 6, TDataType.STRING, true));
        columns.add(new TColumn("s-port", 7, TDataType.LONG, true));
        columns.add(new TColumn("cs-method", 8, TDataType.STRING, true));
        columns.add(new TColumn("cs-uri-stem", 9, TDataType.STRING, true));
        columns.add(new TColumn("cs-uri-query", 10, TDataType.STRING, true));
        columns.add(new TColumn("sc-status", 11, TDataType.LONG, true));
        columns.add(new TColumn("sc-bytes", 12, TDataType.STRING, true));
        columns.add(new TColumn("cs-bytes", 13, TDataType.STRING, true));
        columns.add(new TColumn("time-taken", 14, TDataType.STRING, true));
        columns.add(new TColumn("cs(User-Agent)", 15, TDataType.STRING, true));
        columns.add(new TColumn("cs(Referrer)", 16, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setSeparatorChars(" ");
        separator.setTitleSeparatorChars(" ");
        separator.setLogFormat("date time c-ip cs-username s-ip s-port cs-method cs-uri-stem cs-uri-query sc-status sc-bytes cs-bytes time-taken cs(User-Agent) cs(Referrer)");
        separator.setColumns(columns);
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.EXTENDED_LOG_FORMAT);
        Map<Integer, DataCell> result = separator.separate("" +
                "2002-05-24 " +
                "20:18:01 " +
                "172.224.24.114 " +
                "- " +
                "206.73.118.24 " +
                "80 " +
                "GET " +
                "/Default.htm " +
                "- " +
                "200 " +
                "7930 " +
                "248 " +
                "31 " +
                "Mozilla/4.0+(compatible;+MSIE+5.01;+Windows+2000+Server) " +
                "http://64.224.24.114/");

        Assert.assertEquals(200l, result.get(11).getData());
        Assert.assertEquals(80l, result.get(7).getData());
        Assert.assertEquals("/Default.htm", result.get(9).getData());
    }

    @Test
    public void testIIsLog() {
        Set<TColumn> columns = new HashSet<>();
        columns.add(new TColumn("time", 1, TDataType.STRING, true, true));
        columns.add(new TColumn("c-ip", 2, TDataType.STRING, true, true));
        columns.add(new TColumn("cs-method", 3, TDataType.STRING, true, true));
        columns.add(new TColumn("cs-uri-stem", 4, TDataType.STRING, true));
        columns.add(new TColumn("sc-status", 5, TDataType.STRING, true));
        FileRecordSeparator separator = new FileRecordSeparator();
        separator.setSeparatorChars(",");
        separator.setTitleSeparatorChars(",");
        separator.setLogFormat("time,c-ip ,cs-method ,cs-uri-stem, sc-status");
        separator.setColumns(columns);
        separator.setFileType("LOG");
        separator.setLogFormatType(LogFormatType.EXTENDED_LOG_FORMAT);
        Map<Integer, DataCell> result = separator.separate("00:13:03,100.30.184.000,OPTIONS,/,200");
        Assert.assertEquals("100.30.184.000", result.get(2).getData());
        Assert.assertEquals("/", result.get(4).getData());

        result = separator.separate("22:36:56,100.30.184.1,GET,/WebSites/default.htm,401");
        Assert.assertEquals("100.30.184.1", result.get(2).getData());
        Assert.assertEquals("/WebSites/default.htm", result.get(4).getData());
    }

//    public static void main(String[] args) throws ParseException {
//        SimpleDateFormat iidDateTimeFormat = DateUtil.genSDF("yy/dd/MM HH:mm:ss");
//        System.out.println(iidDateTimeFormat.parse("14/20/01 7:55:20"));
//    }


}

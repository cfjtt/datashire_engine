package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.enumeration.LogFormatType;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.ApacheLogFormat;
import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.engine.util.ExtendedLogFormat;
import com.eurlanda.datashire.enumeration.FileType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * 文件 分割 信息
 * Created by Juntao.Zhang on 2014/5/15.
 */
public class FileRecordSeparator implements Serializable {
    private static Log logger = LogFactory.getLog(FileRecordSeparator.class);
    private SquidTypeEnum squidType;
    private String fileType;
    // 指定column 根据id排序
    private Set<TColumn> columns;

    // ###################### 文档抽取 参数 ######################
    // 文件中数据（记录）的格式(0:Delimited,1:FixedLength, 2:Txt)
    private int rowFormat;
    // 分隔符
    private String delimiter;
    // 字段长度
    private int fieldLength;
    // header的字段number
    private int headerRowNo;
    // 数据抽取行 第一条数据记录
    private int firstDataRowNo;
    private RowDelimiterPosition position;
    private String rowDelimiter;
    // ###################### xml抽取 参数 ########################
    /**
     * <?xml version="1.0" encoding="UTF-8"?>
     * <catalog>
     * <book sku="434_asd"> <title>1984</title> <author>George Orwell</author> <price>12.95</price> <category>classics</category> </book>
     * <book sku="876_pep"> <title>Java Generics and Collections</title> <author>Maurice Naftalin</author> <price>34.99</price> <category>programming</category></book>
     * </catalog>
     * 分割路劲 catalog/book
     */
    private String xmlElementPath;

    // ###################### 网页日志抽取 参数 ######################
    private LogFormatType logFormatType;
    private String logFormat;//为空表示default 预留字段 为后面自定义服务
    private SimpleDateFormat dateFormat;

    //标题分割字符
    private String titleSeparatorChars = " ";
    //value分割字符
    private String separatorChars = " ";
    private PostProcess postProcess;

    public FileRecordSeparator() {
    }

    //txt pdf word
    public FileRecordSeparator(int rowFormat, String delimiter, int fieldLength, int firstDataRowNo, Set<TColumn> columns) {
        this.rowFormat = rowFormat;
        this.delimiter = delimiter;
        this.fieldLength = fieldLength;
        this.firstDataRowNo = firstDataRowNo;
        this.columns = columns;
    }

    //xml
    public FileRecordSeparator(Set<TColumn> columns, String xmlElementPath) {
        this.columns = columns;
        this.xmlElementPath = xmlElementPath;
    }

    //excel
    public Map<Integer, DataCell> separate(List<String> line) {
        Map<Integer, DataCell> result = new HashMap<>();
        if (CollectionUtils.isEmpty(line)) return result;
        for (TColumn col : columns) {
            int index = Integer.parseInt(col.getName());
            Object obj;
            if(line.size()>index) {
                obj = getValue(line.get(index), col);
            } else {
                obj = null;
            }
            result.put(col.getId(), new DataCell(col.getData_type(), obj));
        }
        return result;
    }

    public Map<Integer, DataCell> separate(String line) {
        Map<Integer, DataCell> result = new HashMap<>();
        if (StringUtils.isBlank(line)) return result;
        FileType ft = FileType.valueOf(fileType.toUpperCase());
        switch (ft) {
            case CSV:
            case TXT:
            case DOC:
            case DOCX:
            case PDF:
                String[] dataColumns = null;
                // 如果RowFormat=0，那么说明是用分隔符Delimited来分割
                if (rowFormat == 0) {
                    if(ft == FileType.CSV) {
                        List<String> strs = com.eurlanda.datashire.engine.util.StringUtils.splitCSV(line);
                        dataColumns = strs.toArray(new String[]{});
                    } else {
                        dataColumns = line.split(delimiter);
                    }
                }// 如果RowFormat=1，那么说明是用固定长度FixedLength来分割
                else if (rowFormat == 1) {
                    // 去掉小数之后，自动加1
                    int rowTotal = (int) Math.ceil(line.length() / new Float(fieldLength));
                    dataColumns = new String[rowTotal];
                    for (int j = 0, i = 0; i < dataColumns.length; j += fieldLength, i++) {
                        // 最后一列,直接取到最后, 避免最后一列的长度不够
                        if(i == dataColumns.length - 1) {
                            dataColumns[i] = line.substring(j, line.length());
                        } else {
                            dataColumns[i] = line.substring(j, j + fieldLength);
                        }
                    }
                }
                // 如果RowFormat=2，那么说明源文件是txt,整个txt中的内容就代表一个column定义
                else if (rowFormat == 2) {
                    dataColumns = new String[1];
                    dataColumns[0] = line;
                }
                /**
                 * 没有的列，设空值
                 */
                for (TColumn col : columns) {
                    if(!col.isSourceColumn()) {
                        if(col.getName().equals("extration_date")) {
                            result.put(col.getId(), new DataCell(col.getData_type(), col.getExtractTime()));
                        } else {
                            throw new RuntimeException("不存在非抽取列：" + col);
                        }
                        continue;
                    }
                    int index = Integer.parseInt(col.getName());
                    int dcSize = -1;
                    if(dataColumns != null) {
                        dcSize = dataColumns.length-1;
                    }
                    Object obj;
                    if(index > dcSize) {
                        obj = null;
                    } else {
                        obj = getValue(dataColumns[index], col);
                    }
                    result.put(col.getId(), new DataCell(col.getData_type(), obj));
                }
                break;
            case LOG:
                try {
                    logFormat(line, result);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
                throw new RuntimeException("unhandled file type :" + fileType);

        }
        return result;
    }

    //web log 处理逻辑
    private void logFormat(String line, Map<Integer, DataCell> result) {
        switch (logFormatType) {
            case COMMON_LOG_FORMAT:
                if (StringUtils.isBlank(logFormat)) {
                    logFormat = ApacheLogFormat.DEFAULT_COMMON_LOG_FORMAT;
                }
            case APACHE_COMBINED_FORMAT:
                if (StringUtils.isBlank(logFormat)) {
                    logFormat = ApacheLogFormat.APACHE_COMBINED_FORMAT;
                }
                apacheLog(line, result, logFormat);
                break;
            case EXTENDED_LOG_FORMAT:
                extendedLog(line, result);
                break;
            case IIS_LOG_FILE_FORMAT:
                iisLog(line, result);
                break;
        }
    }

    //iis log handler
    private void iisLog(String line, Map<Integer, DataCell> result) {
        String[] src = StringUtils.split(line, ",");
        Map<String, String> tmp = new HashMap<>();
        tmp.put("client_ip", StringUtils.trim(src[0]));
        tmp.put("user_name", StringUtils.trim(src[1]));
        tmp.put("request_date", StringUtils.trim(src[2]));
        tmp.put("request_time", StringUtils.trim(src[3]));
        tmp.put("service_instance", StringUtils.trim(src[4]));
        tmp.put("server_name", StringUtils.trim(src[5]));
        tmp.put("server_ip", StringUtils.trim(src[6]));
        tmp.put("time_taken", StringUtils.trim(src[7]));
        tmp.put("client_bytes_sent", StringUtils.trim(src[8]));
        tmp.put("server_bytes_sent", StringUtils.trim(src[9]));
        tmp.put("service_status_code", StringUtils.trim(src[10]));
        tmp.put("windows_status_code", StringUtils.trim(src[11]));
        tmp.put("request_type", StringUtils.trim(src[12]));
        tmp.put("operation_target", StringUtils.trim(src[13]));
        tmp.put("parameters", StringUtils.trim(src[14]));
        for (TColumn col : columns) {
            String data = tmp.get(col.getName());
            try {
                switch (col.getData_type()) {
                    case STRING:
                        result.put(col.getId(), new DataCell(col.getData_type(), data));
                        break;
                    case LONG:
                        result.put(col.getId(), new DataCell(col.getData_type(), Long.valueOf(data)));
                        break;
                    case DOUBLE:
                        result.put(col.getId(), new DataCell(col.getData_type(), Double.valueOf(data)));
                        break;
                    case BOOLEAN:
                        result.put(col.getId(), new DataCell(col.getData_type(), data.equalsIgnoreCase("true")
                                || data.equals("1")
                                || data.equalsIgnoreCase("y")));
                        break;
//                    case TIME:
                    case TIMESTAMP:
                        if (col.getName().equals("request_time")) {
                            String date = tmp.get("request_date");
                            result.put(col.getId(), new DataCell(col.getData_type(), DateUtil.parseByIidDateTimeFormat(StringUtils.trim(date + " " + data))));
                        } else if (col.getName().equals("request_date")) {
                            result.put(col.getId(), new DataCell(col.getData_type(), DateUtil.parseByIidDateFormat(StringUtils.trim(data))));
                        }
                        break;
                }
            } catch (Exception e) {
                logger.error("iis log 解析：" + e.getMessage(), e);
            }
        }
    }

    //extended log fields flag
    private static final String EXTENDED_LOG_FIELDS_BEGIN = "#Fields: ";
    private String[] extendedLogFormatNames;

    //处理 Extended Log Format
    private void extendedLog(String line, Map<Integer, DataCell> result) {
        // 头信息
        if (extendedLogFormatNames == null && line.startsWith("#")) {
            //第4行为头信息 显示哪些列
            if(line.startsWith(EXTENDED_LOG_FIELDS_BEGIN)) {
                line = line.replace(EXTENDED_LOG_FIELDS_BEGIN, "");
                extendedLogFormatNames = StringUtils.split(line, separatorChars);
            }
            return;
        }

        //第4行为头信息
//        if (extendedLogFormatNames == null && line.startsWith(EXTENDED_LOG_FIELDS_BEGIN)) {
//            line = line.replace(EXTENDED_LOG_FIELDS_BEGIN, "");
//            extendedLogFormatNames = StringUtils.split(line, separatorChars);
//            return;
//        }
        String[] values = StringUtils.split(line, separatorChars);
        Map<String, String> tmp = new HashMap<>();
        if (extendedLogFormatNames.length != values.length) {
            result = new HashMap<>();
        }
        int i = 0;
        for (String str : extendedLogFormatNames) {
            tmp.put(StringUtils.trim(str), StringUtils.trim(values[i++]));
        }
        for (TColumn col : columns) {
            String key = ExtendedLogFormat.getKey(col.getName());
            String data = tmp.get(key);
            try {
                if (key.equals("time")) {
                    result.put(col.getId(), new DataCell(col.getData_type(), DateUtil.util2Time(DateUtil.parseByDefaultTimeFormat(StringUtils.trim(data)))));
                } else if (key.equals("date")) {
                    result.put(col.getId(), new DataCell(col.getData_type(), DateUtil.util2Timestamp(DateUtil.parseByDefaultDateFormat(StringUtils.trim(data)))));
                } else {
                    result.put(col.getId(), new DataCell(col.getData_type(), getValue(StringUtils.trim(data), col)));
                }
            } catch (ParseException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private String[] apacheLogFormatNames = null;


    //处理apache 两种log
    private void apacheLog(String line, Map<Integer, DataCell> result, String logFormat) {
        Map<String, String> apacheLogs;
        if (ArrayUtils.isEmpty(apacheLogFormatNames)) {
            logFormat = org.apache.commons.lang.StringUtils.trim(logFormat);
            apacheLogFormatNames = org.apache.commons.lang.StringUtils.split(logFormat, " ");
        }
        apacheLogs = ApacheLogFormat.logInfoHandler(line, apacheLogFormatNames);
        String key, data;
        for (TColumn col : columns) {
            key = col.getName();
            switch (key.toUpperCase()) {
                case "REQUEST":
                case "REFERER":
                case "USER_AGENT":
                    data = apacheLogs.get(key);
                    if (data == null) break;
                    result.put(col.getId(), new DataCell(col.getData_type(), getValue(data, col)));
                    break;
                case "REQUEST_DATETIME":
                    data = apacheLogs.get(key);
                    if (data == null) break;
                    try {
                        if (dateFormat == null) {
                            // 此处是线程安全的
                            dateFormat = DateUtil.genSDF("[dd/MMM/yyyy:HH:mm:ss z]", Locale.US);
                        }
                        result.put(col.getId(), new DataCell(col.getData_type(), DateUtil.util2Timestamp(dateFormat.parse(data))));
                    } catch (Exception e) {
                        logger.error(e);
                    }
                    break;
                default:
                    data = apacheLogs.get(key);
                    if (data == null) break;
                    result.put(col.getId(), new DataCell(col.getData_type(), getValue(data, col)));
                    break;
            }
        }
    }

    public Object getValue(String data, TColumn col) {
        return TColumn.toTColumnValue(data, col);
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public int getRowFormat() {
        return rowFormat;
    }

    public void setRowFormat(int rowFormat) {
        this.rowFormat = rowFormat;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    public int getHeaderRowNo() {
        return headerRowNo;
    }

    public void setHeaderRowNo(int headerRowNo) {
        this.headerRowNo = headerRowNo;
    }

    public int getFirstDataRowNo() {
        // 后台将开始行号从-1改为1, 因此需要将行号减2
        return firstDataRowNo - 2;
    }

    public void setFirstDataRowNo(int firstDataRowNo) {
        this.firstDataRowNo = firstDataRowNo;
    }

    public Set<TColumn> getColumns() {
        return columns;
    }

    public void setColumns(Set<TColumn> columns) {
        this.columns = columns;
    }

    public void initPath() throws EngineException {
        if (StringUtils.isBlank(xmlElementPath)) throw new EngineException("xml path is empty.");
        xmlElements = StringUtils.split(xmlElementPath, "/");

    }

    //xmlElementPath 根据/分割成数组
    private String[] xmlElements = null;
    //递归游标
    private Stack<String> cursorPath = new Stack<>();

    public RowDelimiterPosition getPosition() {
        return position;
    }

    public void setPosition(RowDelimiterPosition position) {
        this.position = position;
    }

    public String getRowDelimiter() {
        return rowDelimiter;
    }

    public void setRowDelimiter(String rowDelimiter) {
        this.rowDelimiter = rowDelimiter;
    }

    /**
     * 递归保护
     * 防止死循环
     */
    private static class RecursiveProtected {
        private final static int MAX_DEEP = 20;
        int deep = 0;
        List<Object> message = new ArrayList<>();

        void add() throws EngineException {
            deep++;
            if (MAX_DEEP < deep) throw new EngineException("xml 解析递归异常。");
        }

        public void sub() {
            deep--;
        }

        @Override
        public String toString() {
            return "RecursiveProtected{" +
                    "deep=" + deep +
                    ", message=" + message +
                    '}';
        }
    }

    //StAX方法解析XML
    // <author>William Shakespeare</author>
    public XMLEvent read(XMLEvent xmlEvent, XMLEventReader reader, Map<String, String> infoMap, String startElementName, RecursiveProtected recursiveProtected) throws XMLStreamException, EngineException {
        beforeRead(infoMap, startElementName, recursiveProtected);
        while (isNotEndElement(xmlEvent) && reader.hasNext()) {
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                cursorPath.push(startElement.getName().getLocalPart());
                setAttrs(infoMap, startElement);
                xmlEvent = reader.nextEvent();
                xmlEvent = read(xmlEvent, reader, infoMap, startElement.getName().getLocalPart(), recursiveProtected);
            } else if (xmlEvent.isCharacters()
                    && !xmlEvent.asCharacters().isWhiteSpace()) {
                if(infoMap.containsKey(startElementName)){
                    infoMap.put(startElementName,infoMap.get(startElementName)+StringUtils.trim(xmlEvent.asCharacters().getData()));
                } else {
                    infoMap.put(startElementName,StringUtils.trim(xmlEvent.asCharacters().getData()));
                }
                xmlEvent = reader.nextEvent();
            } else if (xmlEvent.isEndElement()) {
                cursorPath.pop();
            } else {
                xmlEvent = reader.nextEvent();
            }
        }
        afterRead(recursiveProtected);
        return xmlEvent;
    }

    //获得属性值
    //<user title="bbb" name="Tom" age="28" gender="male">Manager</user>
    private void setAttrs(Map<String, String> result, StartElement startElement) {
        Iterator attrs = startElement.getAttributes();
        while (attrs.hasNext()) {
            Attribute attr = (Attribute) attrs.next();
            result.put(attr.getName().getLocalPart(), attr.getValue());
        }
    }

    private void beforeRead(Map<String, String> result, String startElementName, RecursiveProtected protect) throws EngineException {
        protect.add();
        List<Object> condition = new ArrayList<>();
        condition.add(result);
        condition.add(startElementName);
        protect.message.add(condition);
    }

    private void afterRead(RecursiveProtected recursiveProtected) {
        recursiveProtected.sub();
    }

    public Map<Integer, DataCell> separate(XMLEvent xmlEvent, XMLEventReader reader) throws XMLStreamException, EngineException {
        Map<Integer, DataCell> result = new HashMap<>();
        if (xmlEvent == null) return result;
        Map<String, String> infoMap = separateInternal(xmlEvent, reader);
        if (MapUtils.isEmpty(infoMap)) return result;
        for (TColumn col : columns) {
            String val = infoMap.get(col.getName());
            if (StringUtils.isNotBlank(val)) {
                result.put(col.getId(), new DataCell(col.getData_type(), getValue(val, col)));
            }
        }
        return result;
    }

    public Map<String, String> separateInternal(XMLEvent xmlEvent, XMLEventReader reader) throws XMLStreamException, EngineException {
        if (xmlElements == null) {
            initPath();
        }
        Map<String, String> infoMap = new HashMap<>();
        //jump to break element start
        while (!isStartBreakElement() && reader.hasNext()) {
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                cursorPath.push(startElement.getName().getLocalPart());
                if (isStartBreakElement()) {
                    setAttrs(infoMap, startElement);
                }
            } else if (xmlEvent.isEndElement()) {
                cursorPath.pop();
            }
            xmlEvent = reader.nextEvent();
        }
        if (!reader.hasNext()) return infoMap;
        try {
            //如果break element结束跳出循环
            while (isNotEndBreakElement(xmlEvent) && reader.hasNext()) {
                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    cursorPath.push(startElement.getName().getLocalPart());
                    setAttrs(infoMap, startElement);
                    xmlEvent = reader.nextEvent();
                    RecursiveProtected recursiveProtected = new RecursiveProtected();
                    xmlEvent = read(xmlEvent, reader, infoMap, startElement.getName().getLocalPart(), recursiveProtected);
                    logger.debug("递归数据 :" + recursiveProtected);
//                    System.out.println("递归数据 :" + recursiveProtected);
                } else if (xmlEvent.isEndElement()) {
                    cursorPath.pop();
                    xmlEvent = reader.nextEvent();
                } else if (xmlEvent.isCharacters()
                        && !xmlEvent.asCharacters().isWhiteSpace()) {
                    infoMap.put(cursorPath.get(cursorPath.size() - 1), StringUtils.trim(xmlEvent.asCharacters().getData()));
                    xmlEvent = reader.nextEvent();
                } else {
                    xmlEvent = reader.nextEvent();
                }
            }
            //修改路径游标路径
            if (!isNotEndBreakElement(xmlEvent) && !cursorPath.empty()) cursorPath.pop();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return infoMap;
    }

    private boolean isNotEndElement(XMLEvent xmlEvent) {
        return !(
                xmlEvent.isEndElement()
                        && xmlEvent.asEndElement().getName().getLocalPart().equals(cursorPath.get(cursorPath.size() - 1))
        );
    }

    private boolean isNotEndBreakElement(XMLEvent xmlEvent) {
        return !(
                cursorPath.size() == xmlElements.length
                        && xmlEvent.isEndElement()
                        && xmlEvent.asEndElement().getName().getLocalPart().equals(cursorPath.get(cursorPath.size() - 1))
        );
    }

    private boolean isStartBreakElement() {
        if (cursorPath.empty()) return false;
        if (cursorPath.size() != xmlElements.length) return false;
        for (int i = 0; i < cursorPath.size(); i++) {
            if (!cursorPath.get(i).equals(xmlElements[i])) return false;
        }
        return true;
    }

    public SquidTypeEnum getSquidType() {
        return squidType;
    }

    public void setSquidType(SquidTypeEnum squidType) {
        this.squidType = squidType;
    }

    public LogFormatType getLogFormatType() {
        return logFormatType;
    }

    public void setLogFormatType(LogFormatType logFormatType) {
        this.logFormatType = logFormatType;
    }

    public String getLogFormat() {
        return logFormat;
    }

    public void setLogFormat(String logFormat) {
        this.logFormat = logFormat;
    }

    public SimpleDateFormat getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(SimpleDateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getSeparatorChars() {
        return separatorChars;
    }

    public void setSeparatorChars(String separatorChars) {
        this.separatorChars = separatorChars;
    }

    public String getTitleSeparatorChars() {
        return titleSeparatorChars;
    }

    public void setTitleSeparatorChars(String titleSeparatorChars) {
        this.titleSeparatorChars = titleSeparatorChars;
    }

    public String getXmlElementPath() {
        return xmlElementPath;
    }

    public void setXmlElementPath(String xmlElementPath) {
        this.xmlElementPath = xmlElementPath;
    }

    public PostProcess getPostProcess() {
        return postProcess;
    }

    public void setPostProcess(PostProcess postProcess) {
        this.postProcess = postProcess;
    }

    @Override
    public String toString() {
        return "FileLineSeparator{" +
                "squidType=" + squidType +
                ", fileType='" + fileType + '\'' +
                ", columns=" + columns +
                ", rowFormat=" + rowFormat +
                ", delimiter='" + delimiter + '\'' +
                ", fieldLength=" + fieldLength +
                ", headerRowNo=" + headerRowNo +
                ", firstDataRowNo=" + getFirstDataRowNo() +
                ", xmlElementPath='" + xmlElementPath + '\'' +
                ", logFormatType=" + logFormatType +
                ", logFormat='" + logFormat + '\'' +
                ", dateFormat=" + dateFormat +
                ", iidDateFormat='yy/dd/MM'" +
                ", iidDateTimeFormat='yy/dd/MM HH:mm:ss'" +
                ", titleSeparatorChars='" + titleSeparatorChars + '\'' +
                ", separatorChars='" + separatorChars + '\'' +
                ", postProcess=" + postProcess +
                ", extendedLogFormatNames=" + Arrays.toString(extendedLogFormatNames) +
                ", apacheLogFormatNames=" + Arrays.toString(apacheLogFormatNames) +
                ", xmlElements=" + Arrays.toString(xmlElements) +
                ", cursorPath=" + cursorPath +
                '}';
    }
}

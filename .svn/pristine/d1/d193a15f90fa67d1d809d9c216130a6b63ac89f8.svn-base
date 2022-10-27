package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.FileRecordSeparator;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.THdfsSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.enumeration.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhudebin on 16/1/28.
 */
public class HdfsGzTest extends AbstractSquidTest {

    @Test
    public void testGZExtract() {

        TSquidFlow tSquidFlow = new TSquidFlow();
        tSquidFlow.setDebugModel(false);

        DocExtractSquid dc = new DocExtractSquid();
        dc.setPost_process(PostProcess.DO_NOTHING.getValue());
        dc.setDelimiter(",");
        dc.setField_length(0);
        // 压缩编码
        dc.setCompressiconCodec(1);
        dc.setDoc_format(FileType.TXT.value());
        dc.setFirst_data_row_no(1);
        dc.setHeader_row_no(0);
        dc.setRow_delimiter_position(RowDelimiterPosition.End.getValue());
        dc.setRow_delimiter("\n");
        dc.setRow_format(0);






        Set<TColumn> orderList = new HashSet<>();
        orderList.add(new TColumn(1, "1", TDataType.STRING, true, 10, 10));
        orderList.add(new TColumn(2, "2", TDataType.STRING, true, 10, 10));
        orderList.add(new TColumn(3, "3", TDataType.STRING, true, 10, 10));
        orderList.add(new TColumn(4, "4", TDataType.STRING, true, 10, 10));

        FileRecordSeparator fs = new FileRecordSeparator();
        fs.setPostProcess(PostProcess.parse(dc.getPost_process()));
        fs.setColumns(orderList);
        fs.setDateFormat(null);
        fs.setDelimiter(dc.getDelimiter());
        fs.setFieldLength(dc.getField_length());
        fs.setFileType(FileType.parse(dc.getDoc_format()).toString());
        fs.setFirstDataRowNo(dc.getFirst_data_row_no());
        fs.setHeaderRowNo(dc.getHeader_row_no());
        fs.setPosition(RowDelimiterPosition.parse(dc.getRow_delimiter_position()));
        fs.setRowDelimiter(dc.getRow_delimiter());
        fs.setRowFormat(dc.getRow_format());

        THdfsSquid hs = new THdfsSquid();
        hs.setFileType(FileType.TXT.name());
        hs.setIp("192.168.137.102:8020");
//        hs.setPort("8020");
//        hs.setPaths(new String[]{"/eurlanda/test_data/sample_tree_data.gz"});
        hs.setPaths(new String[]{"/eurlanda/test_data/sample_tree_data.csv"});
        hs.setName("hdfs gz extract");
        hs.setFileLineSeparator(fs);

        hs.setCurrentFlow(tSquidFlow);
//        hs.setCodec("gzip");

        hs.run(sc);
        JavaRDD<Map<Integer, DataCell>> outRDD = hs.getOutRDD();
        List<Map<Integer, DataCell>> list = outRDD.collect();

        long count = list.size();
        System.out.println("------------" + count + "------------");
        for(Map<Integer, DataCell> m : list) {
            printMap(m);
        }

    }
}

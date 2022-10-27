package com.eurlanda.datashire.hbase;

import com.eurlanda.datashire.engine.mr.phoenix.parse.ConditionParser;
import com.eurlanda.datashire.engine.util.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 16/2/25.
 */
public class ScanTest {

    @Test
    public void test1() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "e101,e102,e103");

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 5, 2, true, false, "colfam1");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));
        // vv FilterListExample
        List<Filter> filters = new ArrayList<Filter>();

        Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("row-03")));
        filters.add(filter1);

        Filter filter2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("row-06")));
        filters.add(filter2);

        Filter filter3 = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("col-0[03]"));
        filters.add(filter3);

        FilterList filterList1 = new FilterList(filters);

        Scan scan = new Scan();
        scan.setFilter(filterList1);
        ResultScanner scanner1 = table.getScanner(scan);
        // ^^ FilterListExample
        System.out.println("Results of scan #1 - MUST_PASS_ALL:");
        int n = 0;
        // vv FilterListExample
        for (Result result : scanner1) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
                // ^^ FilterListExample
                n++;
                // vv FilterListExample
            }
        }
        scanner1.close();

        FilterList filterList2 = new FilterList(
                FilterList.Operator.MUST_PASS_ONE, filters);

        scan.setFilter(filterList2);
        ResultScanner scanner2 = table.getScanner(scan);
        // ^^ FilterListExample
        System.out.println("Total cell count for scan #1: " + n);
        n = 0;
        System.out.println("Results of scan #2 - MUST_PASS_ONE:");
        // vv FilterListExample
        for (Result result : scanner2) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
                // ^^ FilterListExample
                n++;
                // vv FilterListExample
            }
        }
        scanner2.close();
        // ^^ FilterListExample
        System.out.println("Total cell count for scan #2: " + n);
    }

    @Test
    public void testExpression() throws SQLException, IOException {
        Filter fl = ConditionParser.parseCondition("cf1.name = 'name-05' or  rowkey>20.0 and rowkey<30.0");
        Configuration conf = getConfig();

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Scan scan = new Scan();
        scan.setFilter(fl);

        ResultScanner rs = table.getScanner(scan);
        printlnResultScanner(rs);
        rs.close();
        connection.close();
    }

    @Test
    public void testPageScan() throws Exception {
        Configuration conf = getConfig();

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Scan scan = new Scan();

        Filter filter = new PageFilter(100l);

        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        printlnResultScanner(rs);
        rs.close();
        connection.close();
    }

    @Test
    public void testScanRowFilter() throws IOException {
        Configuration conf = getConfig();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Scan scan = new Scan();
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(200)));
        scan.setFilter(rowFilter);
        ResultScanner rs = table.getScanner(scan);
        printlnResultScanner(rs);
        rs.close();
        connection.close();
    }

    @Test
    public void testDropTable() throws IOException {
        Configuration conf = getConfig();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
    }

    @Test
    public void testCreateTable() throws IOException {
        Configuration conf = getConfig();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.createTable("testtable", "cf1", "cf2");
    }

    private static Configuration getConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "e101,e102,e103");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }

    private static void printlnResultScanner(ResultScanner rs) {
        long n = 0;
        for (Result result : rs) {
            n ++;
            System.out.println("======== row :" + n);
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        }
        System.out.println(" 总数 为: " + n);
    }

    @Test
    public void test10() {
        byte[] bs = Bytes.toBytes("h");

        System.out.println(Bytes.toInt(bs));
    }

    @Test
    public void test11() {
        Configuration conf = getConfig();
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            TableName[] names = admin.listTableNames();
            for(TableName tn : names) {
                System.out.println(tn);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.enumeration.CDCSystemColumn;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.DatabaseUtils;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * cdc
 * Created by Juntao.Zhang on 2014/6/21.
 */
public class CDCSquidFlowTest extends AbstractSquidTest {
    private boolean truncateExistingData = true;
    private int squidFlowId = 1;
    private int databaseSquidId = 1;
    private int dataFallSquidId = 20;
    static TDataSource dataFallDataSource;
    static TDataSource cdcSource =
            new TDataSource("192.168.137.2", 3306, "unit_test", "root", "root", "cdc_source", DataBaseType.MYSQL);
    static TDataSource cdcSource2 =
            new TDataSource("192.168.137.2", 3306, "unit_test", "root", "root", "cdc_source2", DataBaseType.MYSQL);
    static TDataSource cdcSource3 =
            new TDataSource("192.168.137.2", 3306, "unit_test", "root", "root", "cdc_source3", DataBaseType.MYSQL);
    static TDataSource cdcSource4 =
            new TDataSource("192.168.137.2", 3306, "unit_test", "root", "root", "cdc_source4", DataBaseType.MYSQL);

    private static void createDate() {
        try {
            Connection conn = DatabaseUtils.getConnection(cdcSource);
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("DROP TABLE IF EXISTS `cdc_source`");
            conn.createStatement().executeUpdate("CREATE TABLE `cdc_source` ( `pri_key` bigint(10) DEFAULT NULL, `bus_key_1` varchar(255) COLLATE utf8_bin DEFAULT NULL, `bus_key_2` varchar(255) COLLATE utf8_bin DEFAULT NULL, `data_time_cdc2` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP, `name` varchar(255) COLLATE utf8_bin DEFAULT NULL, `age_cdc1` int(5) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source` VALUES ('1', '1', '1', '2014-06-21 18:10:33', 'a', '22')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source` VALUES ('2', '1', '2', '2014-06-21 18:10:33', 'b', '23')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source` VALUES ('3', '1', '3', '2014-06-21 18:10:34', 'v', '24')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source` VALUES ('4', '2', '1', '2014-06-21 18:10:36', 'd', '25')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source` VALUES ('5', '2', '2', '2014-06-21 18:10:38', 'e', '26')");

            conn.createStatement().executeUpdate("DROP TABLE IF EXISTS `cdc_source2`");
            conn.createStatement().executeUpdate("CREATE TABLE `cdc_source2` ( `pri_key` bigint(10) DEFAULT NULL, `bus_key_1` varchar(255) COLLATE utf8_bin DEFAULT NULL, `bus_key_2` varchar(255) COLLATE utf8_bin DEFAULT NULL, `data_time_cdc2` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP, `name` varchar(255) COLLATE utf8_bin DEFAULT NULL, `age_cdc1` int(5) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source2` VALUES ('11', '1', '1', '2014-06-21 18:10:45', 'a', '23')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source2` VALUES ('22', '1', '2', '2014-06-21 18:10:49', 'b', '24')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source2` VALUES ('23', '1', '3', '2014-06-21 18:10:50', 'v', '25')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source2` VALUES ('24', '2', '1', '2014-06-21 18:10:51', 'd', '26')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source2` VALUES ('25', '2', '2', '2014-06-21 18:10:54', 'e', '27')");

            conn.createStatement().executeUpdate("DROP TABLE IF EXISTS `cdc_source3`");
            conn.createStatement().executeUpdate("CREATE TABLE `cdc_source3` ( `pri_key` bigint(10) DEFAULT NULL, `bus_key_1` varchar(255) COLLATE utf8_bin DEFAULT NULL, `bus_key_2` varchar(255) COLLATE utf8_bin DEFAULT NULL, `data_time_cdc2` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP, `name` varchar(255) COLLATE utf8_bin DEFAULT NULL, `age_cdc1` int(5) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source3` VALUES ('31', '1', '1', '2014-06-21 18:11:00', 'a', '33')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source3` VALUES ('32', '1', '2', '2014-06-21 18:11:01', 'b', '34')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source3` VALUES ('33', '1', '3', '2014-06-21 18:11:02', 'v', '35')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source3` VALUES ('34', '2', '1', '2014-06-21 18:11:03', 'd', '36')");
            conn.createStatement().executeUpdate("INSERT INTO `cdc_source3` VALUES ('35', '2', '2', '2014-06-21 18:11:05', 'e', '37')");

            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMysqlCDC() throws Exception {
        dataFallDataSource =
                new TDataSource("192.168.137.2", 3306, "unit_test", "root", "root", "test_cdc_target", DataBaseType.MYSQL);
//        DatabaseUtils.dropTable(dataFallDataSource);
        testCDC();
    }

    private void testCDC() throws EngineException  {
        TSquidFlow flow1 = createSquidFlow(cdcSource);
        flow1.run(sc);
        writeData(flow1);
        TSquidFlow flow2 = createSquidFlow(cdcSource2);
        flow2.run(sc);
        writeData(flow2);
        TSquidFlow flow3 = createSquidFlow(cdcSource3);
        flow3.run(sc);
        writeData(flow3);
        TSquidFlow flow4 = createSquidFlow(cdcSource4);
        flow4.run(sc);
        writeData(flow4);
    }

    @Test
    public void testHbaseCDC() throws Exception {
        dataFallDataSource =
                new TDataSource(ConfigurationUtil.getInnerHbaseHost(),
                        ConfigurationUtil.getInnerHbasePort(),
                        "", "", "", "test_cdc_target", DataBaseType.HBASE_PHOENIX);
        DatabaseUtils.dropTable(dataFallDataSource);
        testCDC();
    }
    @Test
    public void testSqlServerCDC() throws Exception {
        dataFallDataSource =
                new TDataSource("192.168.137.1", 1433,
                        "EngineUnitTest", "squiding@eurlanda", "sa", "test_cdc_target", DataBaseType.SQLSERVER);
        DatabaseUtils.dropTable(dataFallDataSource);
        testCDC();
    }

    private void writeData(TSquidFlow flow3) {
        TDataFallSquid dataFallSquid = (TDataFallSquid) flow3.getSquidList().get(flow3.getSquidList().size() - 1);
        try {
            Connection conn = DatabaseUtils.getConnection(dataFallSquid.getDataSource());
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery("select * from " + dataFallSquid.getDataSource().getTableName());
            System.out.println("\n===================result===================");
            while (rs.next()) {
                System.out.print("");
                for (TColumn c : dataFallSquid.getColumnSet()) {
                    System.out.print(c.getName() + "        : " + rs.getObject(c.getName())+"   ；   ");
                }
                System.out.println("");
            }
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private TSquidFlow createSquidFlow(TDataSource dataSource) {
        TSquidFlow squidFlow = createSquidFlow(1, squidFlowId);
        TDatabaseSquid source = createDatabaseSquid(dataSource);
        TDataFallSquid dataFallSquid = createDataFallSquid(source);
        squidFlow.addSquid(source);
        squidFlow.addSquid(dataFallSquid);
        squidFlowId++;
        databaseSquidId++;
        dataFallSquidId++;
        truncateExistingData = false;
        return squidFlow;
    }

    private TDatabaseSquid createDatabaseSquid(TDataSource dataSource) {
        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId(String.valueOf(databaseSquidId));
        personSquid.setSquidId(databaseSquidId);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("bus_key_1", 1, TDataType.STRING, false));
        personSquid.putColumn(new TColumn("bus_key_2", 2, TDataType.STRING, false));
        personSquid.putColumn(new TColumn("data_time_cdc2", 3, TDataType.TIMESTAMP, false));
        personSquid.putColumn(new TColumn("name", 4, TDataType.STRING, false));
        personSquid.putColumn(new TColumn("age_cdc1", 5, TDataType.LONG, false));
        personSquid.putColumn(new TColumn("pri_key", 10, TDataType.STRING, false, true));
        return personSquid;
    }

    private TDataFallSquid createDataFallSquid(TSquid preSquid) {
        dataFallDataSource.setCDC(true);
        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId(String.valueOf(dataFallSquidId));
        dataFallSquid.setSquidId(dataFallSquidId);
        dataFallSquid.setTruncateExistingData(truncateExistingData);

        Set<TColumn> columnSet = new HashSet<>();
        columnSet.add(new TColumn("bus_key_1", 1, TDataType.STRING, false, -1, false, true, true));
        columnSet.add(new TColumn("bus_key_2", 2, TDataType.STRING, false, -1, false, true, true));
        columnSet.add(new TColumn("data_time_cdc2", 3, TDataType.TIMESTAMP, false, 2, false, false, true));
        columnSet.add(new TColumn("name", 4, TDataType.STRING, false, 2, false, false, true));
        columnSet.add(new TColumn("age_cdc1", 5, TDataType.LONG, false, 1, false, false, true));
        columnSet.add(new TColumn("pri_key", 10, TDataType.STRING, false, -1, true, false, true));

        columnSet.add(new TColumn(CDCSystemColumn.DB_START_DATE_COL_NAME.getColValue(), 11, TDataType.TIMESTAMP, false, -1, false, false, false));
        columnSet.add(new TColumn(CDCSystemColumn.DB_END_DATE_COL_NAME.getColValue(), 12, TDataType.TIMESTAMP, true, -1, false, false, false));
        columnSet.add(new TColumn(CDCSystemColumn.DB_FLAG_COL_NAME.getColValue(), 13, TDataType.STRING, false, -1, false, false, false));
        columnSet.add(new TColumn(CDCSystemColumn.DB_VERSION_COL_NAME.getColValue(), 14, TDataType.LONG, false, -1, false, false, false));

        dataFallSquid.setColumnSet(columnSet);
        dataFallSquid.setDataSource(dataFallDataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }

}

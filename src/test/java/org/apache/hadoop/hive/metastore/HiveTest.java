package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetColumnsResp;
import org.apache.thrift.TException;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by zhudebin on 2017/5/4.
 */
public class HiveTest {

    @Test
    public void test1() throws Exception {
        HiveConf hiveConf = new HiveConf();
        //        HiveClientCache cache = new HiveClientCache(1000);
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);

        /**
        ThriftHiveMetastore.Iface client = hiveMetaStoreClient.client;
        TGetColumnsResp colResp;
        TGetColumnsReq colReq = new TGetColumnsReq();
        colReq.setSessionHandle(null);
        colReq.setCatalogName("default");
        colReq.setSchemaName("default");
        colReq.setTableName("t_user1");
        colReq.setColumnName("");
        try {
            colResp = client.GetColumns(colReq);
        } catch (TException e) {
            throw new SQLException(e.getMessage(), "08S01", e);
        }
//        Utils.verifySuccess(colResp.getStatus());


        Table table = client.getTable("default", "t_user1");
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        List<FieldSchema> cols = table.getSd().getCols();
        System.out.println(table);
         */
        hiveMetaStoreClient.close();
    }

}

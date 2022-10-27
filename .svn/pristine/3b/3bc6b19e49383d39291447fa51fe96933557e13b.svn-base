package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * hbase 抽取squid
 *
 * Created by zhudebin on 16/2/22.
 */
public class THBaseSquid extends TSquid {

    {
        this.setType(TSquidType.HBASE_SQUID);
    }

    private static Log log = LogFactory.getLog(THBaseSquid.class);

    private String url;
    private String tableName;
    // hbase ,调用的 convertScanToString(Scan)
    private String scan;
    // 导出来的列,必须包含值的字段: data_type{导出的数据类型},name{导入列的名字 cf1:name},
    // id{导出的ID},(isSourceColumn{是否是从上游reference导入},extractTime{抽取时间})
    private List<TColumn> columns;
    // connection squid id
    private Integer connectionSquidId;

    @Override
    protected Object run(JavaSparkContext jsc) throws EngineException {

        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(),this, connectionSquidId);

        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.set("hbase.zookeeper.quorum", url.split(":")[0]);
        conf.set("hbase.zookeeper.property.clientPort", url.split(":")[1]);
        conf.set(TableInputFormat.SCAN_COLUMNS, genColumns(columns));
        if(StringUtils.isNotEmpty(scan)) {
            conf.set(TableInputFormat.SCAN, scan);
        }
        JavaPairRDD<ImmutableBytesWritable, Result> rdd = jsc.newAPIHadoopRDD(conf,
                TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        final Broadcast<List<TColumn>> bcolumns = jsc.broadcast(columns);

        this.outRDD = rdd.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,Result>>, Map<Integer, DataCell>>() {
                    final List<TColumn> columnList = bcolumns.value();
                    @Override
                    public Iterator<Map<Integer, DataCell>> call(final Iterator<Tuple2<ImmutableBytesWritable, Result>> tuple2Iterator)
                            throws Exception {
                        return new Iterator<Map<Integer, DataCell>>() {
                            @Override public boolean hasNext() {
                                return tuple2Iterator.hasNext();
                            }

                            @Override
                            public Map<Integer, DataCell> next() {
                                Tuple2<ImmutableBytesWritable, Result> t2 = tuple2Iterator.next();

                                Map<Integer, DataCell> map = new HashMap<>();

                                for (TColumn c : columnList) {
                                    if (c.isSourceColumn()) {
                                        byte[] byteData = null;
                                        if (c.getName().equalsIgnoreCase(
                                                ConstantsUtil.HBASE_ROW_KEY)) {
                                            byteData = t2._1().get();
                                        } else {
                                            byte[][] byte_cq =
                                                    KeyValue.parseColumn(
                                                            c.getName().trim()
                                                                    .getBytes());
                                            byteData = t2._2().getValue(byte_cq[0],
                                                            byte_cq[1]);
                                        }
                                        if (byteData != null) {
                                            map.put(c.getId(),
                                                    new DataCell(c.getData_type(),
                                                            TColumn.convertBinary(
                                                                    byteData,
                                                                    c.getData_type())));
                                        } else {
                                            map.put(c.getId(),
                                                    new DataCell(c.getData_type(),
                                                            null));
                                        }
                                    }
                                }
                                return map;


                            }

                            @Override
                            public void remove() {
                                tuple2Iterator.remove();
                            }
                        };
                    }
                });
        bcolumns.unpersist();
        // 处理异常
//        handleException();
        return this.outRDD;
    }

    /**
     * 不需要了
     */
//    @Deprecated
//    private void handleException() {
//        if (isExceptionHandle) {
//            outRDD = outRDD.persist(StorageLevel.MEMORY_AND_DISK());
//            expOutRDD = outRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
//                @Override
//                public Boolean call(Map<Integer, DataCell> in) throws Exception {
//                    return in != null && in.size() != 0 && in.containsKey(ConstantsUtil.ERROR_KEY);
//                }
//            }).map(new Function<Map<Integer, DataCell>, Map<Integer, DataCell>>() {
//                @Override
//                public Map<Integer, DataCell> call(Map<Integer, DataCell> in) throws Exception {
//                    Map<Integer, DataCell> result = new HashMap<>();
//                    for (Map.Entry<Integer, DataCell> entry : in.entrySet()) {
//                        if (entry.getKey() != ConstantsUtil.ERROR_KEY) {
//                            result.put(entry.getKey(), entry.getValue());
//                        }
//                    }
//                    return result;
//                }
//            });
//        }
//
//        //filter right data
//        outRDD = outRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
//            @Override
//            public Boolean call(Map<Integer, DataCell> in) throws Exception {
//                return !in.containsKey(ConstantsUtil.ERROR_KEY);
//            }
//        });
//    }

    /**
     * 将sourceColumn 的名字用空格间隔组装起来
     * @param columns
     * @return
     */
    private String genColumns(List<TColumn> columns) {

        StringBuilder sb = new StringBuilder();

        boolean isFirst = true;

        for(TColumn c : columns) {

            if(c.isSourceColumn()) {
                // rowkey 不需要放到column 中
                if(c.isSourceColumn() && !ConstantsUtil.HBASE_ROW_KEY.equals(c.getName())) {
                    if(isFirst) {
                        isFirst = false;
                    } else {
                        sb.append(" ");
                    }
                    sb.append(c.getName());
                }


            }
        }

        return sb.toString();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<TColumn> getColumns() {
        return columns;
    }

    public void setColumns(
            List<TColumn> columns) {
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getScan() {
        return scan;
    }

    public void setScan(String scan) {
        this.scan = scan;
    }

    public Integer getConnectionSquidId() {
        return connectionSquidId;
    }

    public void setConnectionSquidId(Integer connectionSquidId) {
        this.connectionSquidId = connectionSquidId;
    }
}

package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.DSUtil;
import com.eurlanda.datashire.engine.util.OrderUtil;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * group by 操作
 * 先group 再聚合？
 * Created by zhudebin on 14-1-11.
 */
public class TAggregateSquid_o extends TSquid implements Serializable {
    transient private static Logger logger = Logger.getLogger(TAggregateSquid.class);
    // group by 的列
    List<Integer> groupKeyList = new ArrayList<>();
    // 聚合操作
    private List<AggregateAction> aaList;
    // 上一个SQUID
    private TSquid previousSquid;

    public TAggregateSquid_o() {
        this.setType(TSquidType.AGGREGATE_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException  {
        // 判断是否可以执行
        if (previousSquid.getOutRDD() == null) {
            previousSquid.run (jsc);
        }
        // 判断是否已经执行 TODO 此处是因为没有落地squid,暂时返回
        if (this.outRDD != null) {
            System.out.println("此处不应该呀....." + this.getSquidId());
            return outRDD;
        }


        // 分组
        JavaPairRDD<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>> jpr = previousSquid.getOutRDD().groupBy(new Function<Map<Integer, DataCell>, ArrayList<DataCell>>() {
            @Override
            public ArrayList<DataCell> call(Map<Integer, DataCell> map) throws Exception {
                ArrayList<DataCell> keyGroup = new ArrayList<>();
                // 判断是否有group by
                if (groupKeyList != null && groupKeyList.size() > 0) {
                    for (Integer i : groupKeyList) {
                        if (map.get(i) == null) {
                            logger.error("====== 此处不能为空 ==============" + map);
                        } else {
                            keyGroup.add(map.get(i));
                        }
                    }
                } else {
                    // 如果没有分组，那么所有数据分成一组
                    // TODO 默认分组采用的reduce 分组合并，可以考虑使用map端reduce再分组
                }

                return keyGroup;
            }
        });

        this.outRDD = jpr.map(new Function<Tuple2<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>>, Map<Integer, DataCell>>() {
            @Override
            public Map<Integer, DataCell> call(Tuple2<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>> t2) throws Exception {
                // 聚合操作实际运行
                return aggregate(t2._2(), aaList);
            }
        });
        return this.outRDD;
    }

    /**
     * 对分组后的list进行聚合
     *
     * @param in     分组后的一组数据
     * @param aaList 聚合操作
     * @return 对一组数据聚合的结果，一组对应一条数据
     */
    private Map<Integer, DataCell> aggregate(Iterable<Map<Integer, DataCell>> in, List<AggregateAction> aaList) {
        Map<Integer, DataCell> outMap = new HashMap<>();

        // 聚合主要计算种类，0、group的列；1、求总数；2、求和；3、最大；4、最小值；5、排序第一个；6、排序最后一个；
        // 对应column所需的聚合动作
        Map<Integer, Map<Integer, DataCell>> aggMap = new HashMap<>();

        // 给聚合动作分类，对同一列数据的聚合，分在一起，（求和、最大、最小、GROUP、总数）
//        logger.debug("聚类动作数为：" + aaList.size());
        List<TOrderItem> orders = null;
        for (AggregateAction aa : aaList) {
            if(aa.getOrders() != null) {
                if(orders == null) {
                    orders = aa.getOrders();
                } else {
                    throw new RuntimeException("一个stageSquid中只能有一个 (first_value|last_value)聚合");
                }
            }
//            logger.debug("聚类动作：" + aa.getInKey() + "," + aa.getType());
            Map<Integer, DataCell> mapValue = aggMap.get(aa.getInKey());
            if (mapValue == null) {
                mapValue = new HashMap<>();
                aggMap.put(aa.getInKey(), mapValue);
            }
            // 将对应的聚合操作合并
            for (Integer i : aa.getType().getaKey()) {
                mapValue.put(i, null);
            }
        }

        // 聚合
        long total = 0;
        Iterator<Map<Integer, DataCell>> iter = in.iterator();
        while (iter.hasNext()) {
            total ++;
            Map<Integer, DataCell> dataMap = iter.next();
            // inKey, aggregateKey, Datacell
            for (Map.Entry<Integer, Map<Integer, DataCell>> aggEntry : aggMap.entrySet()) {
                for (Map.Entry<Integer, DataCell> een : aggEntry.getValue().entrySet()) {
                    // 聚合结果
                    DataCell aggregateResult = een.getValue();
                    // 聚合的类型
                    Integer aggregateType = een.getKey();
                    // 聚合的列
                    Integer aggregateColumn = aggEntry.getKey();
                    switch (aggregateType) {
                        // group column
                        case 0:
                            // 如果等于空，取第一行数据
                            if (aggregateResult == null) {
                                // 判断这一行数据是否为空
                                if (dataMap.get(aggregateColumn) == null) {
                                    logger.error("=========== 空值组 ================= 数据为：" + dataMap);
                                    logger.error("=========== 空值组 ================= 聚合为：" + aggEntry);
                                } else {
                                    aggregateResult = dataMap.get(aggregateColumn).clone();
                                }
                                een.setValue(aggregateResult);
                            }
                            break;
                        // 求总数
                        case 1:
//                            total ++;
                            if (aggregateResult == null) {
                                aggregateResult = new DataCell();
                                aggregateResult.setdType(TDataType.LONG);
                                aggregateResult.setData(total);
                                // todo 暂时没有测试
                                een.setValue(aggregateResult);
                            } else {
                                aggregateResult.setData(total);
                            }
                            break;
                        // 取和
                        case 2:
                            if (aggregateResult == null) {
                                if(DSUtil.isNotNull(dataMap.get(aggregateColumn))) {
                                    aggregateResult = dataMap.get(aggregateColumn).clone();
                                } else {

                                }
                                een.setValue(aggregateResult);

                            } else {
                                aggregateResult.add(dataMap.get(aggregateColumn));
                            }
                            break;
                        // 最大值
                        case 3:
                            if (aggregateResult == null) {
                                aggregateResult = dataMap.get(aggregateColumn).clone();
                                een.setValue(aggregateResult);
                            } else {
                                aggregateResult.max(dataMap.get(aggregateColumn));
                            }
                            break;
                        // 最小值
                        case 4:
                            if (aggregateResult == null) {
                                aggregateResult = dataMap.get(aggregateColumn).clone();
                                een.setValue(aggregateResult);
                            } else {
                                aggregateResult.min(dataMap.get(aggregateColumn));
                            }
                            break;
                        // 根据排序字段求最大值
                        case 5:
                            if (aggregateResult == null) {
                                // 保存当前行的值，用于与下一次的排序比较
                                aggregateResult = new DataCell();
                                aggregateResult.setData(dataMap);
                                een.setValue(aggregateResult);
                            } else {
                                // 根据排序字段比较当前值
                                int compareResult = OrderUtil.compare((Map<Integer, DataCell>)aggregateResult.getData(), dataMap, orders);
                                if(compareResult < 0) {
                                    aggregateResult.setData(dataMap);
                                }
                            }
                            break;
                        // 根据排序字段求最小值
                        case 6:
                            if (aggregateResult == null) {
                                // 保存当前行的值，用于与下一次的排序比较
                                aggregateResult = new DataCell();
                                aggregateResult.setData(dataMap);
                                een.setValue(aggregateResult);
                            } else {
                                // 根据排序字段比较当前值
                                int compareResult = OrderUtil.compare((Map<Integer, DataCell>)aggregateResult.getData(), dataMap, orders);
                                if(compareResult > 0) {
                                    aggregateResult.setData(dataMap);
                                }
                            }
                            break;
                        default:
                            throw new RuntimeException("没有对应的聚合类型，请联系系统管理员");
                    }
                }

            }
        }


        // 三、封装结果
        for (AggregateAction aa : aaList) {
            Integer inKey = aa.getInKey();
            switch (aa.getType()) {
                case GROUP:
                    outMap.put(aa.getOutKey(),
                            aggMap.get(inKey).get(AggregateAction.Type.GROUP.aggregateKey()));
                    break;
                case AVG:
                    /**
                    DataCell rdc = new DataCell();
                    rdc.setdType(TDataType.DOUBLE);
                    rdc.setData(((float) aggMap.get(inKey).get(AggregateAction.Type.SUM.aggregateKey()).getData())
                            / (Long) aggMap.get(inKey).get(AggregateAction.Type.COUNT.aggregateKey()).getData());
                     */
                    DataCell rdc = avg(aggMap.get(inKey).get(AggregateAction.Type.SUM.aggregateKey()), aggMap.get(inKey).get(AggregateAction.Type.COUNT.aggregateKey()));
                            outMap.put(aa.getOutKey(), rdc);
                    break;
                case SUM:
                    outMap.put(aa.getOutKey(),
                            aggMap.get(inKey).get(AggregateAction.Type.SUM.aggregateKey()));
                    break;
                case MAX:
                    outMap.put(aa.getOutKey(),
                            aggMap.get(inKey).get(AggregateAction.Type.MAX.aggregateKey()));
                    break;
                case MIN:
                    outMap.put(aa.getOutKey(),
                            aggMap.get(inKey).get(AggregateAction.Type.MIN.aggregateKey()));
                    break;
                case COUNT:
                    outMap.put(aa.getOutKey(),
                            aggMap.get(inKey).get(AggregateAction.Type.COUNT.aggregateKey()));
                    break;
                case FIRST_VALUE:
                    Map<Integer, DataCell> rowf = (Map<Integer, DataCell>)aggMap.get(inKey).get(AggregateAction.Type.FIRST_VALUE.aggregateKey());
                    outMap.put(aa.getOutKey(), rowf.get(inKey));
                    break;
                case LAST_VALUE:
                    Map<Integer, DataCell> rowl = (Map<Integer, DataCell>)aggMap.get(inKey).get(AggregateAction.Type.FIRST_VALUE.aggregateKey());
                    outMap.put(aa.getOutKey(), rowl.get(inKey));
                    break;
                default:
                    throw new RuntimeException("没有响应类型：" + aa.getType());
            }
        }

//        logger.error("outMap :" + outMap.entrySet().size());
        return outMap;
    }

    private DataCell avg(DataCell count, DataCell num) {
        DataCell dc = new DataCell();
        Long nums = (Long)num.getData();
        switch (count.getdType()) {
            case LONG:
                dc.setData (((Long)count.getData() + 0.0d) / nums);
                dc.setdType(TDataType.DOUBLE);
                break;
            case FLOAT:
                dc.setData ((float)count.getData() / nums);
                dc.setdType(TDataType.FLOAT);
                break;
            case DOUBLE:
                dc.setData ((double)count.getData() / nums);
                dc.setdType(TDataType.DOUBLE);
                break;
            case INT:
                dc.setData (((int)count.getData() + 0.0d) / nums);
                dc.setdType(TDataType.DOUBLE);
                break;
            case BIG_DECIMAL:
                dc.setData (((BigDecimal)count.getData()).divide(BigDecimal.valueOf(nums), 5));
                dc.setdType(TDataType.BIG_DECIMAL);
                break;
            default:
                throw new RuntimeException("无法对该数据类型[" + count.getdType() + "] 求平均值");
        }
        return dc;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

    public List<Integer> getGroupKeyList() {
        return groupKeyList;
    }

    public void setGroupKeyList(List<Integer> groupKeyList) {
        this.groupKeyList = groupKeyList;
    }

    public List<AggregateAction> getAaList() {
        return aaList;
    }

    public void setAaList(List<AggregateAction> aaList) {
        this.aaList = aaList;
    }
}

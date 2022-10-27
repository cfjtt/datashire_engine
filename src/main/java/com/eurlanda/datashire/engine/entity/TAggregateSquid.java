package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.util.ScalaMethodUtil;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * group by 操作
 * 先group 再聚合？
 * Created by zhudebin on 14-1-11.
 */
public class TAggregateSquid extends TSquid implements Serializable {
    transient private static Logger logger = Logger.getLogger(TAggregateSquid.class);
    // group by 的列
    List<Integer> groupKeyList = new ArrayList<>();
    // 聚合操作
    private List<AggregateAction> aaList;
    // 上一个SQUID
    transient private TSquid previousSquid;

    public TAggregateSquid() {
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


//        // 分组
//        JavaPairRDD<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>> jpr = previousSquid.getOutRDD().groupBy(new Function<Map<Integer, DataCell>, ArrayList<DataCell>>() {
//            @Override
//            public ArrayList<DataCell> call(Map<Integer, DataCell> map) throws Exception {
//                ArrayList<DataCell> keyGroup = new ArrayList<>();
//                // 判断是否有group by
//                if (groupKeyList != null && groupKeyList.size() > 0) {
//                    for (Integer i : groupKeyList) {
//                        if (DSUtil.isNotNull(map.get(i))) {
//                            keyGroup.add(map.get(i));
//                        } else {
//                            keyGroup.add(null);
//                        }
//                    }
//                } else {
//                    // 如果没有分组，那么所有数据分成一组
//                    // TODO 默认分组采用的reduce 分组合并，可以考虑使用map端reduce再分组
//                }
//
//                return keyGroup;
//            }
//        });
//
//        // 在翻译时 对aaList中一个column中既存在goup,又存在 aggregate的，只保留aggregate
//        List<TOrderItem> orders = null;
//        for (AggregateAction aa : aaList) {
//            if (aa.getOrders() != null) {
//                if (orders == null) {
//                    orders = aa.getOrders();
//                } else {
//                    throw new RuntimeException("一个stageSquid中只能有一个 (first_value|last_value)聚合");
//                }
//            }
//        }
//        final List<TOrderItem> finalOrders = orders;
//        this.outRDD = jpr.map(new Function<Tuple2<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>>, Map<Integer, DataCell>>() {
//            @Override
//            public Map<Integer, DataCell> call(Tuple2<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>> t2) throws Exception {
//                // 聚合操作实际运行
//                return aggregate(t2._2().iterator(), aaList, finalOrders);
//            }
//        });
//        this.outRDD = SquidUtil.aggregateRDD(previousSquid.getOutRDD(), groupKeyList, aaList);
        this.outRDD = ScalaMethodUtil.aggregateRDD(previousSquid.getOutRDD(), groupKeyList, aaList).toJavaRDD();
        return this.outRDD;
    }

    /**
     *
//     * @param iter
//     * @param aaList
     * @return

    public Map<Integer, DataCell> aggregate(Iterator<Map<Integer, DataCell>> iter, List<AggregateAction> aaList, List<TOrderItem> orders) {
        Map<Integer, Object> tmpMap = new HashMap<>();

        long count = 0l;
        while(iter.hasNext()) {
            count ++;
            Map<Integer, DataCell> row = iter.next();
            for (AggregateAction aa : aaList) {

                Object value = tmpMap.get(aa.getOutKey());
                int outKey = aa.getOutKey();
                int inKey = aa.getInKey();
                // 聚合列的值
                DataCell dc = row.get(inKey);
                boolean isNotNull = DSUtil.isNotNull(dc);
                // 根据聚合来取值
                switch (aa.getType()) {
                    case SORT:
                        break;
                    case GROUP:
                        if(value == null) {
                            if(isNotNull) {
                                tmpMap.put(outKey, dc.clone());
                            }
                        }
                        break;
                    case AVG:
                        // 判断是否为空
                        if(isNotNull) {
                            if (value == null) {
                                tmpMap.put(outKey, new AvgItem(dc.clone()));
                            } else {
                                ((AvgItem)value).add(dc);
                            }
                        }
                        break;
                    case COUNT:
                        break;
                    case FIRST_VALUE:
                        if(value == null) {
                            tmpMap.put(outKey, row);
                        } else {
                            // 根据排序字段比较当前值
                            int compareResult = OrderUtil.compare((Map<Integer, DataCell>) value, row, orders);
                            if (compareResult < 0) {
                                tmpMap.put(outKey, row);
                            }
                        }
                        break;
                    case LAST_VALUE:
                        if(value == null) {
                            tmpMap.put(outKey, row);
                        } else {
                            // 根据排序字段比较当前值
                            int compareResult = OrderUtil.compare((Map<Integer, DataCell>) value, row, orders);
                            if (compareResult > 0) {
                                tmpMap.put(outKey, row);
                            }
                        }
                        break;
                    case MAX:
                        if(isNotNull) {
                            if (value == null) {
                                tmpMap.put(outKey, dc.clone());
                            } else {
                                ((DataCell)value).max(dc);
                            }
                        }
                        break;
                    case MIN:
                        if(isNotNull) {
                            if (value == null) {
                                tmpMap.put(outKey, dc.clone());
                            } else {
                                ((DataCell)value).min(dc);
                            }
                        }
                        break;
                    case SUM:
                        if(isNotNull) {
                            if (value == null) {
                                tmpMap.put(outKey, dc.clone());
                            } else {
                                ((DataCell)value).add(dc);
                            }
                        }
                        break;
                    default:
                        throw new RuntimeException("没有该类型的聚合:" + aa.getType().name());
                }
            }
        }

        Map<Integer, DataCell> outMap = new HashMap<>();
        int last_first_key = 0;
        for(AggregateAction aa : aaList) {
            // 获取first,last 的输出列
            switch (aa.getType()) {
                case FIRST_VALUE:
                case LAST_VALUE:
                    last_first_key = aa.getOutKey();
                    break;
            }
        }
        for (AggregateAction aa : aaList) {
            int outKey = aa.getOutKey();
            switch (aa.getType()) {
                case AVG:
                    AvgItem ai = (AvgItem)tmpMap.get(outKey);
                    if(ai == null) {
                        outMap.put(outKey, null);
                    } else {
                        outMap.put(outKey, ai.avg());
                    }
                    break;
                case COUNT:
                    outMap.put(outKey, new DataCell(TDataType.LONG, count));
                    break;
                case FIRST_VALUE:
                case LAST_VALUE:
                    outMap.put(outKey, ((Map<Integer, DataCell>)tmpMap.get(outKey)).get(aa.getInKey()));
                    break;
                case SORT:
                    if(last_first_key != 0) {
                        outMap.put(outKey, ((Map<Integer, DataCell>) tmpMap.get(last_first_key)).get(aa.getInKey()));
                    }
                    break;
                default:
                    outMap.put(aa.getOutKey(), (DataCell)tmpMap.get(aa.getOutKey()));
            }

        }

        return outMap;
    }
     */

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

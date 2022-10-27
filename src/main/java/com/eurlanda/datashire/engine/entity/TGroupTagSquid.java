package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.translation.TranslateUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * group(可选项) -> sorted -> tag(按照顺序打标签)
 * 打标签规则:
 *  sorted列 -- tag
 *   0  1   --- 0
 *   1  1   --- 1
 *   1  1   --- 1
 *   0  1   --- 2
 *   0  1   --- 2
 *   1  1   --- 3
 *   3  2   --- 4
 * Created by zhudebin on 2017/3/7.
 */
public class TGroupTagSquid extends TSquid {

    {
        setType(TSquidType.GROUP_TAGGING_SQUID);
    }

    private final static String CN_GROUP_TAG = ConstantsUtil.CN_GROUP_TAG;
//    private final static String CN_GROUP_TAG = "Tag";

    TSquid preTSquid;
    String squidName;
    Map<Integer, TStructField> preId2Columns;
    Map<Integer, TStructField> currentId2Columns;
    List<String> groupColumns;
    List<TOrderItem> sortColumns;
    List<String> tagGroupColumns;

    @Override protected Object run(JavaSparkContext jsc) throws EngineException {
        Dataset<Row> ds = null;
//        dataset.repartition(1).sortWithinPartitions("").mapPartitions(null, null);

        if(preId2Columns != null) {    // 判断是否使用dataframe来过滤
            // 判断上游是否有outDataFrame输出
            if(preTSquid.outDataFrame != null ) {
                ds = preTSquid.outDataFrame;
            } else {
                // 没有dataframe的转换成Dataset
                ds = TJoinSquid.rddToDataFrame(getJobContext().getSparkSession(),
                        squidName, preTSquid.getOutRDD().rdd(),
                        preId2Columns);
            }
        } else {
            throw new RuntimeException("翻译代码异常找程序员");
        }

        Dataset<Row> repDs = null;
        // 1. 分组规则: 设置分组的和没有设置分组的
        if(groupColumns == null || groupColumns.size() == 0) {
            // 1.1. 没有设置分组的,先分成一组
            repDs = ds.repartition(1);
        } else {
            // 1.2. 设置分组的对分组列进行分组,
            // 此处只能保证分组列的值相同的会分到一个partition,
            // 一个partition可能存在[0~n]种组值
            Column[] gColumns = new Column[groupColumns.size()];
            int idx = 0;
            for(String c : groupColumns) {
                gColumns[idx++] = new Column(c);
            }
            repDs = ds.repartition(gColumns);
        }


        // 2. 然后根据排序列排序,
        Dataset<Row> sortDS = repDs.sortWithinPartitions(genSortColumns(sortColumns));

        // 3. 判断是否存在tag列,如果存在,需要改名
        String reNameName = TranslateUtil.genRenameFieldName(sortDS.schema().fields(), CN_GROUP_TAG);
        if(reNameName != null) {
            sortDS = sortDS.withColumnRenamed(CN_GROUP_TAG, reNameName);
        }

        // 4. 然后再根据tagGroup列打标签
        final List<String> tgColumns = tagGroupColumns;
        final List<String> gColumns = groupColumns;

        StructType preStructType = sortDS.schema();
        // 增加 tag列 需要判断是否已经存在tag列
        StructField[] sfs = TranslateUtil.preAddStructFieldToArray(preStructType.fields(), CN_GROUP_TAG);
        StructType nst = new StructType(sfs);
        StructType structType = nst.add(CN_GROUP_TAG, "int", true);

        Dataset<Row> tagDS = sortDS.mapPartitions(new MapPartitionsFunction<Row, Row>() {
            @Override public Iterator<Row> call(final Iterator<Row> input) throws Exception {
                return new Iterator<Row>() {
                    // 保存分组 和 [最后一个groutag值,索引tag]
                    // List<Object>  分组列
                    // Object[] 两个值, 0:该分组下的上一个值; 1:对应生成的索引
                    Map<List<Object>, Object[]> groupIdx = new HashMap<>();
                    // 处理之后的 schema
                    StructType st = null;


                    @Override
                    public boolean hasNext() {
                        return input.hasNext();
                    }

                    @Override public Row next() {
                        Row row = input.next();
                        if(row == null) {
                            throw new RuntimeException("TGroupTagSquid 中出现了没有考虑到的情况,请联系管理员");
                        }

                        if(st == null) {
                            StructType preStructType = row.schema();
                            // 增加 tag列 需要判断是否已经存在tag列
                            StructField[] sfs = TranslateUtil.preAddStructFieldToArray(preStructType.fields(), CN_GROUP_TAG);
                            StructType nst = new StructType(sfs);
                            st = nst.add(CN_GROUP_TAG, "int", true);
                        }
                        Object[] objs = new Object[st.length()];
                        // 判断groupTag列是否连续, 不连续加一,否则不加
                        row.toSeq().copyToArray(objs);

                        // 获取group的值
                        List<Object> groupValues = getValuesFromRow(row, gColumns);
                        List<Object> tagGroupValues = getValuesFromRow(row, tgColumns);

                        // 如果不存在group列,那么所有的都是一个组
                        if(groupValues == null) {
                            groupValues = new ArrayList<>(); // 用空的list代表没有group列
                        }
                        Object[] groupInfo = groupIdx.get(groupValues);

                        int index  = 0;
                        if (groupInfo == null) {
                            // 不存在,该组第一次出现
                            groupIdx.put(groupValues, new Object[]{tagGroupValues, 0});
                        } else if(groupInfo != null && !((List<Object>)groupInfo[0]).equals(tagGroupValues)) {
                            // 存在该组, 并且值不连续
                            index = (Integer) groupInfo[1] + 1;
                            groupInfo[0] = tagGroupValues;
                            groupInfo[1] = index;
                        } else { // 存在但是值连续 不需要变更
                            index = (Integer) groupInfo[1];
                        }
                        objs[st.length() -1 ] = index;

                        Row resultRow = new GenericRowWithSchema(objs, st);
                        return resultRow;
                    }

                    @Override public void remove() {
                        input.remove();
                    }
                };
            }
        }
                , RowEncoder.apply(structType)
//                , org.apache.spark.sql.Encoders.kryo(Row.class)
        );

        outDataFrame = tagDS;
        outRDD = TJoinSquid.groupTaggingDataFrameToRDD(tagDS, currentId2Columns).toJavaRDD();
        return outRDD;
    }

    /**
     * 从Row中取出对应列的值
     * @param row
     * @param tgColumns
     * @return
     */
    private static List<Object> getValuesFromRow(Row row, List<String> tgColumns) {
        if(tgColumns == null || tgColumns.size() == 0) {
            return null;
        }
        List<Object> values = new ArrayList<>();

        for(String cname : tgColumns) {
            int idx = row.fieldIndex(cname);
            values.add(row.get(idx));
        }

        return values;
    }

    /**
     * 根据排序信息生成对应的column数组
     * @param sortColumns
     * @return
     */
    private static Column[]  genSortColumns(List<TOrderItem> sortColumns) {
        Column[] sColumns = new Column[sortColumns.size()];
        int idx = 0;
        for(TOrderItem orderItem : sortColumns) {
            Column c = new Column(orderItem.getName());
            if(orderItem.isAscending()) {
                c = c.asc();
            } else {
                c = c.desc();
            }
            sColumns[idx++] = c;
        }

        return sColumns;
    }

    public TSquid getPreTSquid() {
        return preTSquid;
    }

    public void setPreTSquid(TSquid preTSquid) {
        this.preTSquid = preTSquid;
    }

    public String getSquidName() {
        return squidName;
    }

    public void setSquidName(String squidName) {
        this.squidName = squidName;
    }

    public Map<Integer, TStructField> getPreId2Columns() {
        return preId2Columns;
    }

    public void setPreId2Columns(
            Map<Integer, TStructField> preId2Columns) {
        this.preId2Columns = preId2Columns;
    }

    public List<String> getGroupColumns() {
        return groupColumns;
    }

    public void setGroupColumns(List<String> groupColumns) {
        this.groupColumns = groupColumns;
    }

    public List<TOrderItem> getSortColumns() {
        return sortColumns;
    }

    public void setSortColumns(
            List<TOrderItem> sortColumns) {
        this.sortColumns = sortColumns;
    }

    public List<String> getTagGroupColumns() {
        return tagGroupColumns;
    }

    public void setTagGroupColumns(List<String> tagGroupColumns) {
        this.tagGroupColumns = tagGroupColumns;
    }

    public Map<Integer, TStructField> getCurrentId2Columns() {
        return currentId2Columns;
    }

    public void setCurrentId2Columns(
            Map<Integer, TStructField> currentId2Columns) {
        this.currentId2Columns = currentId2Columns;
    }
}



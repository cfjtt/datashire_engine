package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.server.model.PivotSquid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PivotSquidBuilder extends AbsTSquidBuilder implements TSquidBuilder{
    protected static Logger logger = LoggerFactory.getLogger(PivotSquidBuilder.class);
    public PivotSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override
    public List<TSquid> doTranslate(Squid squid) {
        PivotSquid pivotSquid = (PivotSquid) squid;
        String groupColumnStr = pivotSquid.getGroupByColumnIds();
        int pivotColumnId = pivotSquid.getPivotColumnId();
        int valueColumnId = pivotSquid.getValueColumnId();
        int aggregationType = pivotSquid.getAggregationType();
        String pivotColumnValue = pivotSquid.getPivotColumnValue();
        List<ReferenceColumn> sourceColumns = pivotSquid.getSourceColumns();
        List<Column> columns = pivotSquid.getColumns();

        TPivotSquid tPivotSquid = new TPivotSquid();
        //翻译group列
        List<String> groupColumnList = new ArrayList<>();
        String[] groupColumnIds = groupColumnStr.split(",");
        for(String groupColumn : groupColumnIds){
            ReferenceColumn column = getReferenceColumnById(Integer.parseInt(groupColumn),sourceColumns);
            groupColumnList.add(column.getName());
        }
        //翻译PivotColumn列
        ReferenceColumn pivotRef = getReferenceColumnById(pivotColumnId,sourceColumns);
        TColumn pivotColumn = new TColumn();
        pivotColumn.setId(pivotRef.getColumn_id());
        pivotColumn.setData_type(TDataType.sysType2TDataType(pivotRef.getData_type()));
        pivotColumn.setLength(pivotRef.getLength());
        pivotColumn.setName(pivotRef.getName());
        //翻译valueColumnId
        ReferenceColumn valueRef = getReferenceColumnById(valueColumnId,sourceColumns);
        TColumn valueColumn = new TColumn();
        valueColumn.setId(valueRef.getColumn_id());
        valueColumn.setData_type(TDataType.sysType2TDataType(valueRef.getData_type()));
        valueColumn.setLength(valueRef.getLength());
        valueColumn.setName(valueRef.getName());
        List<Object> pivotColumnValueList = new ArrayList<>();
        //翻译pivotColumnValue
        //将从column中取
        for(Column col : columns){
            //如果不是分组列，那么肯定是pivot列
            if(!col.isIs_groupby()){
                Object value = col.getDescription();
                if(value==null){
                    pivotColumnValueList.add("null");
                } else if("".equals(value)){
                    pivotColumnValueList.add("");
                } else {
                    //根据pivot类型，将具体的值转换成具体类型
                    value = TColumn.toTColumnValue(value + "", pivotColumn.getData_type());
                    pivotColumnValueList.add(value);
                }
            }
        }
        tPivotSquid.setAggregationType(aggregationType);
        tPivotSquid.setGroupColumns(groupColumnList);
        tPivotSquid.setPivotColumn(pivotColumn);
        tPivotSquid.setValueColumn(valueColumn);
        tPivotSquid.setPivotColumnValue(pivotColumnValueList);
        List<Squid> preSquids = ctx.getPrevSquids(squid);
        if(preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("只允许一个上游");
        }
        //设置上游的squid
        TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
        tPivotSquid.setPreviousSquid(preTSquid);
        Map<Integer, TStructField> id2columns = TranslateUtil.getId2ColumnsFromSquid(preSquids.get(0));
        tPivotSquid.setPreId2Columns(id2columns);
        Map<Integer, TStructField> cid2columns = TranslateUtil.getId2ColumnsFromSquid(squid);
        tPivotSquid.setCurrentId2Columns(cid2columns);
        tPivotSquid.setNeedTransColumns(TranslateUtil.getId2ColumnsFromSquidByNameIsRefName(squid));
        return Arrays.asList((TSquid) tPivotSquid);
    }


    private ReferenceColumn getReferenceColumnById(int refColumnId,List<ReferenceColumn> sourceColumns){
        ReferenceColumn column  = null;
        for(ReferenceColumn sourceColumn : sourceColumns){
            if(sourceColumn.getColumn_id()==refColumnId){
                column = sourceColumn;
                break;
            }
        }
        return column;
    }
}

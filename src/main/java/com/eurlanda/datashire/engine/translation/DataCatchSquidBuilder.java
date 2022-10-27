package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.server.model.DataCatchSquid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017-07-04.
 * 把后台的DataCatchSquid翻译成引擎的TDataCatchSquid
 */
public class DataCatchSquidBuilder extends StageBuilder implements TSquidBuilder {
//public class DataCatchSquidBuilder extends AbsTSquidBuilder implements TSquidBuilder {

    protected static Logger logger = LoggerFactory.getLogger(DataCatchSquidBuilder.class);
	public DataCatchSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override public List<TSquid> doTranslate(Squid currentSquid) {

        DataCatchSquid dataCatchSquid = (DataCatchSquid) currentSquid;
        List<Squid> preSquids = ctx.getPrevSquids(dataCatchSquid);
        if (preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("DataViewSquid之前的squid 异常");
        }
        TDataCatchSquid tDataCatchSquid = new TDataCatchSquid();
        tDataCatchSquid.setSquidId(dataCatchSquid.getId());
        tDataCatchSquid.squidName_$eq(dataCatchSquid.getName());
        TSquid previousTSquid= ctx.getSquidOut(preSquids.get(0));
        TSquid previousTSquid2 = null;
        if(previousTSquid instanceof TDebugSquid){ // 直到前面的 Normailzer squid 或pls squid ， 略过其后的内部squid
            previousTSquid2 = ((TInnerExtractSquid)((TDebugSquid) previousTSquid).getPreviousSquid()).getPreTSquid();
        }
        tDataCatchSquid.preSquid_$eq(previousTSquid2);
        tDataCatchSquid.tColumns_$eq(column2TColumns(dataCatchSquid.getColumns()));
        return Arrays.asList((TSquid) tDataCatchSquid);

    }

    public static List<TColumn> column2TColumns(java.util.List<com.eurlanda.datashire.entity.Column> columns) {
        List<TColumn> res = new ArrayList<>();
        for (com.eurlanda.datashire.entity.Column col : columns) {
            res.add(columns2TColumn(col));
        }
        return res;
    }

    /**
     * @param enyColumn
     * @return
     */
    private static TColumn columns2TColumn(com.eurlanda.datashire.entity.Column enyColumn){
        TColumn tcolumn = new TColumn();
        tcolumn.setName(enyColumn.getName());
        tcolumn.setNullable(enyColumn.isNullable());
        tcolumn.setLength(enyColumn.getLength());
        tcolumn.setPrecision(enyColumn.getPrecision());
        tcolumn.setScale(enyColumn.getScale());
        tcolumn.setPrimaryKey(enyColumn.isFK());
        tcolumn.setCdc(enyColumn.getCdc());
        tcolumn.setBusinessKey(enyColumn.getIs_Business_Key() == 0 ? false :true);
        tcolumn.setId(enyColumn.getId());
        tcolumn.setData_type(TDataType.sysType2TDataType(enyColumn.getData_type()));

        return tcolumn;
    }


}

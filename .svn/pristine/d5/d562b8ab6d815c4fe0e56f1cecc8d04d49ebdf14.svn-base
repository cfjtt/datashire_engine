package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TCoefficientSquid;
import com.eurlanda.datashire.engine.entity.TDebugSquid;
import com.eurlanda.datashire.engine.entity.TInnerExtractSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.server.model.DataCatchSquid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017-08-02.
 * 把后台的CoefficientSquid翻译成引擎的TCoefficientSquid，  系数矩阵
 */
public class CoefficientSquidBuilder extends StageBuilder implements TSquidBuilder {

    protected static Logger logger = LoggerFactory.getLogger(CoefficientSquidBuilder.class);
    public CoefficientSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override public List<TSquid> doTranslate(Squid currentSquid) {

        DataCatchSquid coeffSquid = (DataCatchSquid) currentSquid;
        List<Squid> preSquids = ctx.getPrevSquids(coeffSquid);
        if (preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("CoefficientSquid之前的squid 异常");
        }
        TCoefficientSquid tcoefficientSquid = new TCoefficientSquid();
        tcoefficientSquid.setSquidId(coeffSquid.getId());
        tcoefficientSquid.squidName_$eq(coeffSquid.getName());
        TSquid previousTSquid= ctx.getSquidOut(preSquids.get(0));
        TSquid previousTSquid2 = null;
        if(previousTSquid instanceof TDebugSquid){ // 直到前面的 DM squid ， 略过其后的内部squid
            previousTSquid2 = ((TInnerExtractSquid)((TDebugSquid) previousTSquid).getPreviousSquid()).getPreTSquid();
        }
        tcoefficientSquid.preSquid_$eq(previousTSquid2);
        tcoefficientSquid.tColumns_$eq(DataCatchSquidBuilder.column2TColumns(coeffSquid.getColumns()));
        return Arrays.asList((TSquid) tcoefficientSquid);

    }



}

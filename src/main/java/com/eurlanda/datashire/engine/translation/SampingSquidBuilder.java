package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TSamplingSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TStructField;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.server.model.SamplingSquid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 抽样squid翻译
 */
public class SampingSquidBuilder extends AbsTSquidBuilder implements TSquidBuilder{
    protected static Logger logger = LoggerFactory.getLogger(SampingSquidBuilder.class);

    public SampingSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        super(ctx, currentSquid);
    }
    @Override
    public List<TSquid> doTranslate(Squid squid) {
        SamplingSquid samplingSquid = (SamplingSquid) squid;
        TSamplingSquid tsquid = new TSamplingSquid();
        tsquid.setSamplingPercent(samplingSquid.getSamplingPercent());
        tsquid.setSourceSquidId(samplingSquid.getSourceSquidId());
        tsquid.setSquidId(samplingSquid.getId());
        tsquid.setName(samplingSquid.getName());
        tsquid.getSquidList().add(tsquid);
        //sampingsquid只允许一个上游
        List<Squid> preSquids = ctx.getPrevSquids(squid);
        if(preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("SampingSquid 只允许一个上游");
        }
        //设置上游的squid
        TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
        tsquid.setPreviousSquid(preTSquid);
        //设置referenceColumn
        Map<Integer, TStructField> id2columns = TranslateUtil.getId2ColumnsFromSquid(preSquids.get(0));
        tsquid.setPreId2Columns(id2columns);
        //设置currentColumn
        Map<Integer, TStructField> cid2columns = TranslateUtil.getId2ColumnsFromSquid(squid);
        tsquid.setCurrentId2Columns(cid2columns);
        return Arrays.asList((TSquid) tsquid);
    }

}

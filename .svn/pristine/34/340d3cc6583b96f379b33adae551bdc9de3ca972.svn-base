package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TStatisticsSquid;
import com.eurlanda.datashire.engine.entity.TStructField;
import com.eurlanda.datashire.engine.spark.statistics.StatisticsAlgorithmName;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.StatisticsDataMapColumn;
import com.eurlanda.datashire.entity.StatisticsParameterColumn;
import com.eurlanda.datashire.entity.StatisticsSquid;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017-05-09.
 *
 *  把server的统计squid 翻译成引擎的统计squid，用server的squid的对象构建擎的squid对象
 */
public class StatisticsSquidBuilder extends AbsTSquidBuilder implements TSquidBuilder {

    public StatisticsSquidBuilder(BuilderContext builderContext, Squid currentSquid) {
        super(builderContext, currentSquid);
    }

    @Override
    public List<TSquid> doTranslate(Squid squid) {

        StatisticsSquid statisticsSquid = (StatisticsSquid) squid;

        List<Squid> preSquids = ctx.getPrevSquids(squid);
        if (preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("统计Squid之前的squid 异常");
        }

        TStatisticsSquid tStatisticsSquid = new TStatisticsSquid();
        tStatisticsSquid.setSquidId(squid.getId());
        tStatisticsSquid.squidName_$eq(squid.getName());

        TSquid previousTSquid = ctx.getSquidOut(preSquids.get(0));
        tStatisticsSquid.previousSquid_$eq(previousTSquid);
        tStatisticsSquid.statisticsAlgorithmName_$eq(StatisticsAlgorithmName.withName(statisticsSquid.getStatistics_name().trim().toUpperCase()));

        //输入列
        List<StatisticsDataMapColumn> inputDataMapColumns = statisticsSquid.getStatisticsDataMapColumns();
        Map<Integer, TStructField> id2columns = new HashMap<>();
        for (StatisticsDataMapColumn inputCol : inputDataMapColumns) {
            int columnId = inputCol.getColumn_id();
            if(id2columns.containsKey(columnId)){
                throw new IllegalArgumentException(tStatisticsSquid.statisticsAlgorithmName()+"入参不能连接相同列");
            }
            id2columns.put(columnId, new TStructField(inputCol.getName(), TDataType.sysType2TDataType(inputCol.getType()), true, inputCol.getPrecision(), inputCol.getScale()));
        }
        tStatisticsSquid.previousId2Columns_$eq(id2columns);

        //控制参数
        HashMap<String, Object> params = new HashMap<>();
        for (StatisticsParameterColumn paramCol : statisticsSquid.getStatisticsParametersColumns()) {
            params.put(paramCol.getName(), paramCol.getValue());
        }
        tStatisticsSquid.params_$eq(params);

        Map<Integer, TStructField> cid2columns = TranslateUtil.getId2ColumnsFromSquid(squid);
        tStatisticsSquid.currentId2Columns_$eq(cid2columns);

        return Arrays.asList((TSquid) tStatisticsSquid);

    }

}

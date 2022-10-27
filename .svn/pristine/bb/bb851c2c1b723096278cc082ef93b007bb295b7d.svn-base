package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TStructField;
import com.eurlanda.datashire.engine.entity.TUserDefinedSquid;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.UserDefinedMappingColumn;
import com.eurlanda.datashire.entity.UserDefinedParameterColumn;
import com.eurlanda.datashire.entity.UserDefinedSquid;
import scala.Tuple3;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** UserDefinedSquid 翻译
 * Created by zhudebin on 2017/3/8.
 */
public class UserDefinedSquidBuilder extends AbsTSquidBuilder
        implements TSquidBuilder {

    public UserDefinedSquidBuilder(BuilderContext ctx,
            Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override
    public List<TSquid> doTranslate(Squid squid) {

        UserDefinedSquid userDefinedSquid = (UserDefinedSquid)squid;

        List<Squid> preSquids = ctx.getPrevSquids(squid);
        if(preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("USERDEFINED SQUID 前置squid 异常");
        }

        TUserDefinedSquid tUserDefinedSquid = new TUserDefinedSquid();
        tUserDefinedSquid.setSquidId(squid.getId());
        tUserDefinedSquid.setSquidName(squid.getName());

        TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
        tUserDefinedSquid.setPreTSquid(preTSquid);

        // 需要根据输入列映射关系来设定
        List<UserDefinedMappingColumn> mappingColumns = userDefinedSquid.getUserDefinedMappingColumns();
        Map<Integer, TStructField> id2columns = new HashMap<>();
        for(UserDefinedMappingColumn udmc : mappingColumns) {
            int columnId = udmc.getColumn_id();
            id2columns.put(columnId, new TStructField(udmc.getName(), TDataType.sysType2TDataType(udmc.getType()), true, udmc.getPrecision(), udmc.getScale()));
        }

        HashMap<String, String> params = new HashMap<>();
        for(UserDefinedParameterColumn udpc : userDefinedSquid.getUserDefinedParameterColumns()) {
            params.put(udpc.getName(), udpc.getValue());
        }
        tUserDefinedSquid.setParams(params);

        tUserDefinedSquid.setPreId2Columns(id2columns);

        Map<Integer, TStructField> cid2columns = TranslateUtil.getId2ColumnsFromSquid(squid);
        tUserDefinedSquid.setCurrentId2Columns(cid2columns);

        tUserDefinedSquid.setClassName(userDefinedSquid.getSelectClassName());


        return Arrays.asList((TSquid) tUserDefinedSquid);
    }
}

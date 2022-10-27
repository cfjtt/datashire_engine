package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TGroupTagSquid;
import com.eurlanda.datashire.engine.entity.TOrderItem;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TStructField;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.GroupTaggingSquid;
import com.eurlanda.datashire.entity.Squid;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** GroupTaggingSquid 翻译
 * Created by zhudebin on 2017/3/8.
 */
public class GroupTaggingSquidBuilder extends AbsTSquidBuilder
        implements TSquidBuilder {

    public GroupTaggingSquidBuilder(BuilderContext ctx,
            Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override
    public List<TSquid> doTranslate(Squid squid) {

        GroupTaggingSquid gts = (GroupTaggingSquid)squid;

        List<String> groupColumns = new ArrayList<>();
        List<Squid> preSquids = ctx.getPrevSquids(squid);
        if(preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("GROUP TAGGING SQUID 前置squid 异常");
        }
        List<Column> columns = ((DataSquid)preSquids.get(0)).getColumns();
        List<TOrderItem> sortColumns = new ArrayList<>();
        List<String> tagGroupColumns = new ArrayList<>();
        String gcIds = gts.getGroupColumnIds();
        String scIds = gts.getSortingColumnIds();
        String tgIds = gts.getTaggingColumnIds();
        assert scIds != null ;
        assert tgIds != null ;
        if(StringUtils.isEmpty(gcIds)) {
            groupColumns = null;
        } else {
            for(String id: gcIds.split(",")) {
                Column c = getColumnById(columns, Integer.parseInt(id));
                groupColumns.add(c.getName());
            }
        }

        for(String id : scIds.split(",")) {
            Column c = getColumnById(columns, Integer.parseInt(id));
            sortColumns.add(new TOrderItem(c.getName(), true));
        }
        for(String id : tgIds.split(",")) {
            Column c = getColumnById(columns, Integer.parseInt(id));
            tagGroupColumns.add(c.getName());
        }



        TGroupTagSquid tGroupTagSquid = new TGroupTagSquid();
        tGroupTagSquid.setSquidId(squid.getId());
        tGroupTagSquid.setSquidName(squid.getName());


        tGroupTagSquid.setGroupColumns(groupColumns);
        tGroupTagSquid.setSortColumns(sortColumns);
        tGroupTagSquid.setTagGroupColumns(tagGroupColumns);

        TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
        tGroupTagSquid.setPreTSquid(preTSquid);

        Map<Integer, TStructField> id2columns = TranslateUtil.getId2ColumnsFromSquid(preSquids.get(0));
        tGroupTagSquid.setPreId2Columns(id2columns);

        Map<Integer, TStructField> cid2columns = TranslateUtil.getId2ColumnsFromSquid(squid);
        tGroupTagSquid.setCurrentId2Columns(cid2columns);

        return Arrays.asList((TSquid) tGroupTagSquid);
    }

    private Column getColumnById(List<Column> columns, int id) {
        for(Column c : columns) {
            if(c.getId() == id) {
                return c;
            }
        }
        throw new RuntimeException("没有找到column id为" + id + "的列");
    }
}

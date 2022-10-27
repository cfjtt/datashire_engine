package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.THBaseSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.exception.TranslateException;
import com.eurlanda.datashire.engine.mr.phoenix.parse.ConditionParser;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.HBaseConnectionSquid;
import com.eurlanda.datashire.entity.HBaseExtractSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.ScanUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HBaseSquid 翻译的squid链为: THBaseSquid -> TTransformationSquid -> [TDataFallSquid]
 *
 * Created by zhudebin on 16/2/23.
 */
public class HBaseSquidBuilder extends AbsTSquidBuilder implements TSquidBuilder {

    private static Log log = LogFactory.getLog(HBaseSquidBuilder.class);

    public HBaseSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override
    public List<TSquid> doTranslate(Squid squid) {
        HBaseExtractSquid hBaseExtractSquid = (HBaseExtractSquid)squid;
        // 获取上游hbase 连接squid
        HBaseConnectionSquid
                hBaseConnectionSquid = (HBaseConnectionSquid)this.ctx.getPrevSquids(squid).get(0);

        // 翻译 THBaseSquid
        THBaseSquid thBaseSquid = new THBaseSquid();
        thBaseSquid.setUrl(hBaseConnectionSquid.getUrl());
        thBaseSquid.setTableName(getExtractSquidTableName(hBaseExtractSquid.getId(), hBaseExtractSquid.getSource_table_id()));
        thBaseSquid.setConnectionSquidId(hBaseConnectionSquid.getId());

        int filterType = hBaseExtractSquid.getFilterType();
        if(filterType == 0) {   // 表达式
            // 获得 filter 表达式
            String filter = hBaseExtractSquid.getFilter();
            if(StringUtils.isNotEmpty(filter)) {
                // 将表达式转换为 scan
                try {
                    Scan scan = ConditionParser.parseConditionToScan(filter);
                    // 将 scan 转为字符串
                    thBaseSquid.setScan(ScanUtil.convertScanToString(scan));
                } catch (SQLException e) {
                    log.error("过滤表达式转换为Scan异常", e);
                    throw new TranslateException("过滤表达式转换为Scan异常", e);
                } catch (IOException e) {
                    log.error("过滤表达式转换为Scan异常", e);
                    throw new TranslateException("过滤表达式转换为Scan异常", e);
                }
            }
        } else if(filterType == 1) {    // scan
            // 是否需要验证一下scan可以反向成功
            if(StringUtils.isNotEmpty(hBaseExtractSquid.getScan())) {
                thBaseSquid.setScan(hBaseExtractSquid.getScan());
            }
        }

        List<TColumn> columns = new ArrayList<>();
        // 获取 referenceColumn
        for(ReferenceColumn rc : hBaseExtractSquid.getSourceColumns()) {
            // 判断referenceColumn 是否有线连着,有连线的才导出
            Column column = super.getColumnByRefColumnId(hBaseExtractSquid, rc.getColumn_id());
            if(column != null) {
                // 该列需要导出
                TColumn tColumn = new TColumn();
                tColumn.setId(-rc.getColumn_id());
                tColumn.setName(formatColumnName(rc.getName()));
                tColumn.setData_type(TDataType.VARBINARY);
                columns.add(tColumn);
            } else {
                // 该列不需要导出
                log.debug("该RC列不需要导出:" + rc.getName());
            }
        }

        thBaseSquid.setColumns(columns);
        currentBuildedSquids.add(thBaseSquid);

        Map<String, Object> customInfo = new HashMap<>();
        customInfo.put(TTransformationInfoType.HBASE_VARBINARY_CONVERT.dbValue, "hbase");
        TSquid ret12 = doExtractTranslateTransformation(hBaseExtractSquid, customInfo);
        if (ret12 != null)
            currentBuildedSquids.add(ret12);

        TSquid dfSquid = doTranslateDataFall(hBaseExtractSquid);
        if(dfSquid != null) {
            currentBuildedSquids.add(dfSquid);
        }
        // 增量/全量
        TSquid ret6 = doTranslateProcessMode(hBaseExtractSquid);
        if (ret6 != null)
            currentBuildedSquids.add(ret6);
        return this.currentBuildedSquids;
    }

    /**
     * 将名字中的 . 替换成 :
     * @param name
     * @return
     */
    private static String formatColumnName(String name) {

        if(ConstantsUtil.HBASE_ROW_KEY.equals(name)) {
            return name;
        } else {
            byte[][] bytes = parseColumn(name.getBytes(Charset.forName("utf-8")));
            if(bytes.length == 2) {
                return Bytes.toString(KeyValue.makeColumn(bytes[0], bytes[1]));
            } else {
                return name;
            }
        }

    }

    /**
     * Splits a column in {@code family:qualifier} form into separate byte arrays. An empty qualifier
     * (ie, {@code fam:}) is parsed as <code>{ fam, EMPTY_BYTE_ARRAY }</code> while no delimiter (ie,
     * {@code fam}) is parsed as an array of one element, <code>{ fam }</code>.
     * <p>
     * Don't forget, HBase DOES support empty qualifiers. (see HBASE-9549)
     * </p>
     * <p>
     * Not recommend to be used as this is old-style API.
     * </p>
     * @param c The column.
     * @return The parsed column.
     */
    public static byte [][] parseColumn(byte [] c) {
        final int index = KeyValue.getDelimiter(c, 0, c.length, '.');
        if (index == -1) {
            // If no delimiter, return array of size 1
            return new byte [][] { c };
        } else if(index == c.length - 1) {
            // family with empty qualifier, return array size 2
            byte [] family = new byte[c.length-1];
            System.arraycopy(c, 0, family, 0, family.length);
            return new byte [][] { family, HConstants.EMPTY_BYTE_ARRAY};
        }
        // Family and column, return array size 2
        final byte [][] result = new byte [2][];
        result[0] = new byte [index];
        System.arraycopy(c, 0, result[0], 0, index);
        final int len = c.length - (index + 1);
        result[1] = new byte[len];
        System.arraycopy(c, index + 1 /* Skip delimiter */, result[1], 0, len);
        return result;
    }

}

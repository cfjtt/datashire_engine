package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TDestHdfsSquid;
import com.eurlanda.datashire.engine.entity.THdfsPartitionFallSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.spark.util.HdfsSquidUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestHDFSSquid;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 异常处理
 * @author Gene
 *
 */
public class DestHdfsSquidBuilder extends AbsTSquidBuilder {

	public DestHdfsSquidBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}


	public List<TSquid> doTranslateold(Squid squid) {
		DestHDFSSquid destSquid = (DestHDFSSquid) squid;
        TDestHdfsSquid tDestSquid = new TDestHdfsSquid();
        tDestSquid.setFileType(destSquid.getFile_formate());
        List<Integer> idList = new ArrayList<>();
        Map<String, String> params = new HashMap<>();
        params.put(HdfsSquidUtil.HOST(), destSquid.getHost());
        params.put(HdfsSquidUtil.HDFS_PATH(), destSquid.getHdfs_path());
        params.put(HdfsSquidUtil.FILE_FORMATE(), destSquid.getFile_formate() + "");
        params.put(HdfsSquidUtil.COLUMN_DELIMITER(), destSquid.getColumn_delimiter());
        params.put(HdfsSquidUtil.ROW_DELIMITER(), destSquid.getRow_delimiter());
        params.put(HdfsSquidUtil.SAVE_TYPE(), destSquid.getSave_type() + "");
        params.put(HdfsSquidUtil.ZIP_TYPE(), destSquid.getZip_type() + "");
        tDestSquid.setParams(params);

        SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();

        List<DestHDFSColumn> columnList = squidFlowDao.getHDFSColumns(destSquid.getId());
        Collections.sort(columnList, new Comparator<DestHDFSColumn>() {
            @Override public int compare(DestHDFSColumn o1, DestHDFSColumn o2) {
                return o1.getColumn_order() - o2.getColumn_order();
            }
        });
        for(DestHDFSColumn hc : columnList) {
            if(hc.getIs_dest_column() == 1) {
                // 获取上游的column id
                idList.add(hc.getColumn_id());
            }
        }

        tDestSquid.setIdList(idList);

		// 设置

		List<Squid> preSquids = ctx.getPrevSquids(squid);
		if(preSquids == null || preSquids.size() != 1) {
			throw new RuntimeException("DEST ES SQUID 前置squid 异常");
		}
		TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
        tDestSquid.setPreviousSquid(preTSquid);
        tDestSquid.setSquidId(destSquid.getId());

		 currentBuildedSquids.add(tDestSquid);
    	 return this.currentBuildedSquids;
	}


    public List<TSquid> doTranslate(Squid squid) {
        DestHDFSSquid destSquid = (DestHDFSSquid) squid;

        THdfsPartitionFallSquid tDestSquid = new THdfsPartitionFallSquid();
        tDestSquid.setName(squid.getName());
        tDestSquid.setSquidId(squid.getId());
        Map<String,Object> params = new HashMap();
        params.put(HdfsSquidUtil.COLUMN_DELIMITER(),destSquid.getColumn_delimiter());
        params.put(HdfsSquidUtil.COMPRESSION_TYPE_INDEX(),destSquid.getZip_type());
        params.put(HdfsSquidUtil.FALL_FILE_FORMAT_INDEX(),destSquid.getFile_formate());
        params.put(HdfsSquidUtil.HOST(),destSquid.getHost());
        params.put(HdfsSquidUtil.SAVE_HDFS_PATH(),destSquid.getHdfs_path());
        params.put(HdfsSquidUtil.SAVE_MODEL_INDEX(),destSquid.getSave_type());
        tDestSquid.params_$eq(params);

        List<Integer> fallColumnIds = new ArrayList<>();
        List<String> fallColumnNames = new ArrayList<>();
        List<Integer> partitionColumnIds = new ArrayList<>();

        SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();
        List<DestHDFSColumn> destColumnList = squidFlowDao.getHDFSColumns(destSquid.getId());
        Collections.sort(destColumnList, new Comparator<DestHDFSColumn>() {
            @Override
            public int compare(DestHDFSColumn o1, DestHDFSColumn o2) {
                return o1.getColumn_order() - o2.getColumn_order();
            }
        });
        for (DestHDFSColumn hc : destColumnList) {
            if (hc.getIs_dest_column() == 1) { // 落地列,包含分区列
                fallColumnIds.add(hc.getColumn_id());
                fallColumnNames.add(hc.getField_name());
                if (hc.getIs_partition_column() == 1 ) { // 分区列，是部分落地列
                    partitionColumnIds.add(hc.getColumn_id());
                }
            }
        }

        //落地列id
        Integer[] fallColumnIdArr = new Integer[fallColumnIds.size()];
        tDestSquid.fallColumnIds_$eq(fallColumnIds.toArray(fallColumnIdArr));

        //落地列名
        String[] fallColumnNameArr = new String[fallColumnNames.size()];
        tDestSquid.fallColumnNames_$eq(fallColumnNames.toArray(fallColumnNameArr));

        // 获取分区列Ids
        Integer[] partitionColumnIdArr = new Integer[partitionColumnIds.size()];
        tDestSquid.partitionColumnIds_$eq(partitionColumnIds.toArray(partitionColumnIdArr));

        List<Squid> preSquids = ctx.getPrevSquids(squid);
        if (preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("DEST HDFS SQUID 前置squid 异常");
        }
        TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
        tDestSquid.previousSquid_$eq(preTSquid);
        tDestSquid.setSquidId(destSquid.getId());

        currentBuildedSquids.add(tDestSquid);
        return this.currentBuildedSquids;
    }


}

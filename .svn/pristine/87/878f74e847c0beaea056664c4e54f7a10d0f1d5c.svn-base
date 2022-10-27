package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.util.PathUtil;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.EncodingType;
import com.eurlanda.datashire.enumeration.FileType;
import com.eurlanda.datashire.enumeration.HDFSCompressionType;
import com.eurlanda.datashire.utility.EnumException;

import java.util.*;

/**
 * doc抽取。
 * 
 * @author Gene
 * 
 */
public class DocExtractBuilder extends AbsTSquidBuilder implements TSquidBuilder {

	public DocExtractBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid squid) {
		
		DocExtractSquid dc = (DocExtractSquid) squid;

		FileRecordSeparator fs = new FileRecordSeparator();
		List<ReferenceColumn> columns = dc.getSourceColumns();
		Set<TColumn> orderList = new HashSet<>();
		java.util.Collections.sort(columns, new Comparator<ReferenceColumn>() {
			@Override
			public int compare(ReferenceColumn o1, ReferenceColumn o2) {
				return o1.getRelative_order() > o2.getRelative_order() ? 1 : -1;
			}
		});
        int index=0;
		for (ReferenceColumn c : columns) {

			TColumn tColumn = new TColumn(String.valueOf(index++), -c.getColumn_id(), TDataType.STRING);
			Column col = this.getColumnByRefColumnId(dc, c.getColumn_id());
			if(col!=null){
				tColumn.setData_type(TDataType.STRING);
			}
			orderList.add(tColumn);
		}
		
        fs.setPostProcess(PostProcess.parse(dc.getPost_process()));
		fs.setColumns(orderList);
		fs.setDateFormat(null);
		fs.setDelimiter(dc.getDelimiter());
		fs.setRowDelimiter(dc.getRow_delimiter());
		fs.setFieldLength(dc.getField_length());
		fs.setFileType(FileType.parse(dc.getDoc_format()).toString());
		fs.setFirstDataRowNo(dc.getFirst_data_row_no());
		fs.setHeaderRowNo(dc.getHeader_row_no());
        fs.setPosition(RowDelimiterPosition.parse(dc.getRow_delimiter_position()));
		fs.setRowFormat(dc.getRow_format());
		Squid sourceSquid = this.ctx.getPrevSquids(squid).get(0);

		if (sourceSquid instanceof FtpSquid) { // ftp.
			FtpSquid ftpSquid = (FtpSquid) sourceSquid;
			
			TFtpFileSquid tfs = new TFtpFileSquid();
			tfs.setConnectionSquidId(sourceSquid.getId());
			tfs.setIp(PathUtil.getHost(ftpSquid.getHost()));
			tfs.setPort(Integer.parseInt(PathUtil.getPort(ftpSquid.getHost())));
			tfs.setUsername(ftpSquid.getUser_name());
			tfs.setPassword(ftpSquid.getPassword());
//			tfs.setEncoding(EncodingType.parse(ftpSquid.getEncoding()));
			tfs.setServerEncoding(EncodingType.parse(ftpSquid.getEncoding()));
            // 应该是取抽取时的文件编码
			tfs.setEncoding(EncodingType.parse(dc.getEncoding()));
			tfs.setSquidId(squid.getId());
            // 是否允许匿名登录
            tfs.setAnonymous(ftpSquid.getAllowanonymous_flag() == 1);
            // 协议
            tfs.setProtocol(ftpSquid.getProtocol());
            // 传输模式
            tfs.setTransfermode(ftpSquid.getTransfermode_flag());

			tfs.setTaskId(this.ctx.getTaskId());

		     // 需要对 合并所有做判断，如果是合并所有才能取全部的
			JList<String> list = new JList<>();
            if(dc.getUnion_all_flag() == 1) {
                for (SourceTable st : ftpSquid.getSourceTableList()) {
                    list.a(ftpSquid.getFile_path() + "/" + st.getTableName());
                }
            } else {
                for (SourceTable st : ftpSquid.getSourceTableList()) {
                    if(st.getId() == dc.getSource_table_id()) {
                        list.a(ftpSquid.getFile_path() + "/" + st.getTableName());
                        break;
                    }
                }

            }
			tfs.setFiles(list.toArray(new String[0]));
			tfs.setFileLineSeparator(fs);
			tfs.setFileType(FileType.parse(dc.getDoc_format()).toString());
			this.currentBuildedSquids.add(tfs);
		} else if (sourceSquid instanceof FileFolderSquid) { // file folder
			FileFolderSquid fileSquid = (FileFolderSquid) sourceSquid;
			TSharedFileSquid tfs = new TSharedFileSquid();
			tfs.setConnectionSquidId(sourceSquid.getId());
			tfs.setFirstDataRowNo(dc.getFirst_data_row_no());
			tfs.setIp(fileSquid.getHost());
			tfs.setUsername(fileSquid.getUser_name());
			tfs.setPassword(fileSquid.getPassword());
//			tfs.setEncoding(EncodingType.parse(fileSquid.getEncoding()));
            // 设置文件读取时的编码
			tfs.setEncoding(EncodingType.parse(dc.getEncoding()));
			tfs.setSquidId(squid.getId());
			tfs.setTaskId(this.ctx.getTaskId());

            // 需要对 合并所有做判断，如果是合并所有才能取全部的
			JList<String> list = new JList<>();
            if(dc.getUnion_all_flag() == 1) {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                }
            } else {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    if(st.getId() == dc.getSource_table_id()) {
                        list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                        break;
                    }
                }

            }
			tfs.setName(fileSquid.getName());
			tfs.setFiles(list.toArray(new String[0]));
			tfs.setFileLineSeparator(fs);
			// tfs.setFileType("txt");
			tfs.setFileType(FileType.parse(dc.getDoc_format()).toString());
			this.currentBuildedSquids.add(tfs);
		} else if (sourceSquid instanceof HdfsSquid) { // hdfs
			HdfsSquid fileSquid = (HdfsSquid) sourceSquid;
			THdfsSquid tfs = new THdfsSquid();
			tfs.setConnectionSquidId(sourceSquid.getId());
//            tfs.setPort(fileSquid.getPort());
            tfs.setIp(fileSquid.getHost());
			tfs.setName(fileSquid.getName());
			tfs.setSquidId(squid.getId());
			tfs.setTaskId(this.ctx.getTaskId());

			// 需要对 合并所有做判断，如果是合并所有才能取全部的
			JList<String> list = new JList<>();
            if(dc.getUnion_all_flag() == 1) {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                }
            } else {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    if(st.getId() == dc.getSource_table_id()) {
                        list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                        break;
                    }
                }

            }
			tfs.setPaths(list.toArray(new String[0]));
			tfs.setFileLineSeparator(fs);
			tfs.setFileType(FileType.parse(dc.getDoc_format()).toString());
            try {
                tfs.setCodec(HDFSCompressionType.valueOf(dc.getCompressiconCodec()).codec());
            } catch (EnumException e) {
                throw new RuntimeException(e);
            }
            this.currentBuildedSquids.add(tfs);
		}
		Map<String, Object> customInfo = new HashMap<>();
		customInfo.put(TTransformationInfoType.DOC_VARCHAR_CONVERT.dbValue, "doc");
		TSquid ret12 = doExtractTranslateTransformation(dc,customInfo);
		if (ret12 != null) {
            ret12.setSquidId(dc.getId());
            currentBuildedSquids.add(ret12);
        }
		TSquid ret11 = doTranslateSquidFilter(dc);
		if (ret11 != null) {
            ret11.setSquidId(dc.getId());
            currentBuildedSquids.add(ret11);
        }

		TSquid dfSquid = doTranslateDataFall(dc);
		if(dfSquid != null) {
            dfSquid.setSquidId(dc.getId());
			currentBuildedSquids.add(dfSquid);
		}
        // 增量/全量
        TSquid ret6 = doTranslateProcessMode(dc);
        if (ret6 != null) {
            ret6.setSquidId(dc.getId());
            currentBuildedSquids.add(ret6);
        }
		return this.currentBuildedSquids;
	}

	/**
	 * 落地squid.
	 * 
	 * @param
	 * @return

	public TSquid doTranslateDataFall(DocExtractSquid stageSquid) {
		if (stageSquid.getIs_persisted()) { // 设置为落地
			// 默认表结构已经生成了, 表名为： sf_implied.squidFlowId + _ + squidId
			// 1. 获取所有的columns，
			// 2. 根据column信息，翻译成 TColumn
			// 3. TDataSource信息
			Set<TColumn> tColumnMap = new HashSet<>();
			List<Column> columns = stageSquid.getColumns();
			for (Column c : columns) {
				TColumn tColumn = new TColumn();
				tColumn.setId(c.getId());
				tColumn.setName(c.getName());
				tColumn.setLength(c.getLength());
				tColumn.setPrecision(c.getPrecision());
				tColumn.setPrimaryKey(c.isIsPK());
				tColumn.setData_type(TDataType.sysType2TDataType(c.getData_type()));
				tColumn.setNullable(c.isNullable());
				tColumnMap.add(tColumn);
			}

			if (stageSquid.getDestination_squid_id() == 0) { // 设置落地但没有指定落地库。隐式落地。

				TDataSource tDataSource = new TDataSource();

				tDataSource.setType(DataBaseType.HBASE_PHOENIX);
				// tDataSource.setTableName(HbaseUtil.genImpliedTableName(this.ctx.getRepositoryId(),
				// stageSquid.getSquidflow_id(), stageSquid.getId()));
				tDataSource.setTableName(stageSquid.getTable_name());
				tDataSource.setHost(ConfigurationUtil.getInnerHbaseHost() + ":" + ConfigurationUtil.getInnerHbasePort());
//				tDataSource.setPort();

				// TDataSource tDataSource = new TDataSource("192.168.137.2",
				// 3306,
				// "squidflowtest", "root", "root", stageSquid.getSquidflow_id()
				// +
				// "_" + stageSquid.getId(), DataBaseType.MYSQL);

				TDataFallSquid tDataFallSquid = new TDataFallSquid();
				tDataFallSquid.setTruncateExistingData(stageSquid.isTruncate_existing_data_flag() == 0 ? false : true);
				tDataFallSquid.setPreviousSquid(this.getCurLastTSqud());
				tDataFallSquid.setDataSource(tDataSource);
				tDataFallSquid.setSquidId(stageSquid.getId());
				tDataFallSquid.setType(TSquidType.DATA_FALL_SQUID);
				tDataFallSquid.setColumnSet(tColumnMap);
				return tDataFallSquid;

			} else { // 指定地点的落地。

				int squidId = stageSquid.getDestination_squid_id();
				Squid destSquid = ctx.getSquidById(squidId);
				if (SquidTypeEnum.parse(destSquid.getSquid_type()) == SquidTypeEnum.DBSOURCE) {
					DbSquid dbSquid = (DbSquid) destSquid;

					TDataFallSquid tdf = new TDataFallSquid();
					tdf.setTruncateExistingData(stageSquid.isTruncate_existing_data_flag() == 0 ? false : true);
					TDataSource tDataSource = new TDataSource();
					tDataSource.setDbName(dbSquid.getDb_name());
					tDataSource.setUserName(dbSquid.getUser_name());
					tDataSource.setPassword(dbSquid.getPassword());
					tDataSource.setType(DataBaseType.parse(dbSquid.getDb_type()));
					tDataSource.setTableName(stageSquid.getTable_name());
					tDataSource.setHost(dbSquid.getHost());
					tDataSource.setPort(dbSquid.getPort());

					tdf.setPreviousSquid(this.getCurLastTSqud());
					tdf.setDataSource(tDataSource);
					tdf.setSquidId(stageSquid.getId());
					tdf.setType(TSquidType.DATA_FALL_SQUID);

					tdf.setColumnSet(tColumnMap);
					return tdf;
				}
			}
		}
		return null;
	}
	 */

}

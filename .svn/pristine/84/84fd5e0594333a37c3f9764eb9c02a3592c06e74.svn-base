package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.FileRecordSeparator;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TFtpFileSquid;
import com.eurlanda.datashire.engine.entity.THdfsSquid;
import com.eurlanda.datashire.engine.entity.TSharedFileSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.enumeration.LogFormatType;
import com.eurlanda.datashire.engine.enumeration.PostProcess;
import com.eurlanda.datashire.engine.util.PathUtil;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.FileFolderSquid;
import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.XmlExtractSquid;
import com.eurlanda.datashire.enumeration.EncodingType;
import com.eurlanda.datashire.enumeration.FileType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * doc抽取。
 * @author Gene
 *
 */
public class XmlExtractBuilder extends AbsTSquidBuilder implements TSquidBuilder {

	private boolean removeNoUseKeys=false;
	public XmlExtractBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid squid) {
		
		XmlExtractSquid wes = (XmlExtractSquid) squid;
		FileRecordSeparator fs = new FileRecordSeparator();
		List<ReferenceColumn> columns = wes.getSourceColumns();
		Set<TColumn> orderList = new HashSet<>();
		for (ReferenceColumn c : columns) {
			TColumn tColumn = new TColumn(c.getName(),-c.getColumn_id(),TDataType.sysType2TDataType(c.getData_type()));
			tColumn.setPrimaryKey(c.isIsPK());
			Column col = this.getColumnByRefColumnId(wes, c.getColumn_id());
			if(col==null){	
				tColumn.setData_type(TDataType.STRING);
			}else{
				tColumn.setData_type(TDataType.sysType2TDataType(col.getData_type()));
			}
			orderList.add(tColumn);
		}
		fs.setPostProcess(PostProcess.parse(wes.getPost_process()));
        fs.setColumns(orderList);
		fs.setDateFormat(null);
		fs.setFileType(FileType.XML.name());
		fs.setLogFormatType(LogFormatType.parse(wes.getLog_format()));
		fs.setXmlElementPath(wes.getXsd_dtd_path());
	
		Squid sourceSquid = this.ctx.getPrevSquids(squid).get(0);
		if(sourceSquid instanceof FtpSquid){		//	ftp.
			FtpSquid ftpSquid = (FtpSquid) sourceSquid;
			TFtpFileSquid tfs = new TFtpFileSquid();
			tfs.setConnectionSquidId(sourceSquid.getId());
			tfs.setIp(PathUtil.getHost(ftpSquid.getHost()));
			tfs.setPort(Integer.parseInt(PathUtil.getPort(ftpSquid.getHost())));
			tfs.setUsername(ftpSquid.getUser_name());
			tfs.setPassword(ftpSquid.getPassword());
			tfs.setServerEncoding(EncodingType.parse(ftpSquid.getEncoding()));
			tfs.setEncoding(EncodingType.parse(wes.getEncoding()));
			tfs.setSquidId(squid.getId());
			tfs.setTaskId(this.ctx.getTaskId());
            // 是否允许匿名登录
            tfs.setAnonymous(ftpSquid.getAllowanonymous_flag() == 1);
            // 协议
            tfs.setProtocol(ftpSquid.getProtocol());
            // 传输模式
            tfs.setTransfermode(ftpSquid.getTransfermode_flag());

			// 需要对 合并所有做判断，如果是合并所有才能取全部的
			JList<String> list = new JList<>();
            if(wes.getUnion_all_flag() == 1) {
                for (SourceTable st : ftpSquid.getSourceTableList()) {
                    list.a(ftpSquid.getFile_path() + "/" + st.getTableName());
                }
            } else {
                for (SourceTable st : ftpSquid.getSourceTableList()) {
                    if(st.getId() == wes.getSource_table_id()) {
                        list.a(ftpSquid.getFile_path() + "/" + st.getTableName());
                        break;
                    }
                }
            }
			tfs.setFiles(list.toArray(new String[0]));
			tfs.setFileLineSeparator(fs);
			tfs.setFileType("XML");
			this.currentBuildedSquids.add(tfs);
		}else if(sourceSquid instanceof FileFolderSquid){		// file folder
			FileFolderSquid fileSquid = (FileFolderSquid) sourceSquid;
			TSharedFileSquid tfs = new TSharedFileSquid();
			tfs.setConnectionSquidId(sourceSquid.getId());
			tfs.setIp(fileSquid.getHost());
			tfs.setUsername(fileSquid.getUser_name());
			tfs.setPassword(fileSquid.getPassword());
			tfs.setEncoding(EncodingType.parse(fileSquid.getEncoding()));
			tfs.setSquidId(squid.getId());
			tfs.setTaskId(this.ctx.getTaskId());
			
			// 需要对 合并所有做判断，如果是合并所有才能取全部的
			JList<String> list = new JList<>();
            if(wes.getUnion_all_flag() == 1) {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                }
            } else {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    if(st.getId() == wes.getSource_table_id()) {
                        list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                        break;
                    }
                }
            }
			tfs.setName(fileSquid.getName());
			tfs.setFiles(list.toArray(new String[0]));
			tfs.setFileLineSeparator(fs);
			tfs.setFileType("XML");
			this.currentBuildedSquids.add(tfs);
		}else if(sourceSquid instanceof HdfsSquid){		// hdfs
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
            if(wes.getUnion_all_flag() == 1) {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                }
            } else {
                for (SourceTable st : fileSquid.getSourceTableList()) {
                    if(st.getId() == wes.getSource_table_id()) {
                        list.a(fileSquid.getFile_path() + "/" + st.getTableName());
                        break;
                    }
                }
            }
			tfs.setPaths(list.toArray(new String[0]));
			tfs.setFileLineSeparator(fs);
			tfs.setFileType("XML");
			this.currentBuildedSquids.add(tfs);
		}
		TSquid ret11 = doTranslateSquidFilter(wes);
		if (ret11 != null)
			currentBuildedSquids.add(ret11);
		
		TSquid ret12 = doExtractTranslateTransformation(wes);
		if (ret12 != null)
			currentBuildedSquids.add(ret12);
		
		currentBuildedSquids.add(doTranslateDataFall(wes));
		return this.currentBuildedSquids;
	}

	/**
	private TSquid doTranslateTransformation(DataSquid dc) {
		TTransformationSquid ts = new TTransformationSquid();
		List<TTransformationAction> actions = new ArrayList<TTransformationAction>();
		ts.settTransformationActions(actions);
		for(TransformationLink link : dc.getTransformationLinks()){
			Transformation from_trs = this.getTrsById(link.getFrom_transformation_id());
			Transformation to_trs = this.getTrsById(link.getTo_transformation_id());
			TTransformationAction action = new TTransformationAction();
			TTransformation tTransformation = new TTransformation();
			tTransformation.setType(TransformationTypeEnum.VIRTUAL);
			tTransformation.setSequenceIndex(1);
			tTransformation.setInKeyList(new JList<>(-from_trs.getColumn_id()));
			tTransformation.setOutKeyList(new JList<>(to_trs.getColumn_id()));
			Set<Integer> dtk =getDropedTrsKeys(to_trs);
			dtk.add(-from_trs.getColumn_id());
			action.setRmKeys(dtk);
			
			
			Column col = this.getColumnById(to_trs.getColumn_id());
			JMap infoMap = new JMap<>();
			infoMap.put(TTransformationInfoType.STRING_MAX_LENGTH.dbValue, col.getLength());
			infoMap.put(TTransformationInfoType.NUMERIC_PRECISION.dbValue, col.getPrecision());
			infoMap.put(TTransformationInfoType.NUMERIC_SCALE.dbValue, col.getScale());
			tTransformation.setInfoMap(infoMap);
			
			action.settTransformation(tTransformation );
			actions.add(action);
		}
		// 添加 extraction_date
		Column dateCol = getDateColumn();
		if (dateCol != null) {
			TTransformationAction action = new TTransformationAction();
			TTransformation tTransformation = new TTransformation();
			tTransformation.setType(TransformationTypeEnum.CONSTANT);
			tTransformation.setSequenceIndex(1);
			tTransformation.setOutKeyList(new JList<Integer>(dateCol.getId()));
			action.settTransformation(tTransformation);
			JMap infoMap = new JMap<>();
			infoMap.put(TTransformationInfoType.CONSTANT_VALUE.dbValue, DateUtil.nowTimestamp());
			infoMap.put(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue, TDataType.sysType2TDataType(SystemDatatype.DATETIME.value()));
			tTransformation.setInfoMap(infoMap);
			actions.add(action);
		}
		ts.setPreviousSquid(this.getCurLastTSqud());
		return ts;
	}
	private Column getDateColumn() {
		XmlExtractSquid de = (XmlExtractSquid) this.currentSquid;
		for (Column col : de.getColumns()) {
			if (col.getName().equals("extraction_date")) {
				return col;
			}
		}
		return null;
	}
	 */

}

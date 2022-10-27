package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.engine.util.cool.JMap;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.ExceptionSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
/**
 * 异常处理
 * @author Gene
 *
 */
public class ExceptionBuilder extends StageBuilder implements TSquidBuilder{

	private boolean removeNoUseKeys = false;

	public ExceptionBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid squid) {
		ExceptionSquid dc = (ExceptionSquid) squid;
		
		TSquid ret333 = doTranslateExceptionSquid(dc);
		currentBuildedSquids.add(ret333);
		
		
//		TSquid ret11 = doTranslateSquidFilter(dc);
		TSquid ret11 = doTranslateNewSquidFilter(dc);
		if (ret11 != null)
			currentBuildedSquids.add(ret11);
		TSquid ret12 = doTranslateTransformation(dc);
		if (ret12 != null){
			currentBuildedSquids.add(ret12);
		}
		TSquid ret3 = super.doTranslateDataFall(dc);
		if(ret3!= null){
			currentBuildedSquids.add(ret3);
		}

        TSquid ret6 = doTranslateProcessMode(dc);
        if (ret6 != null)
            currentBuildedSquids.add(ret6);

        return this.currentBuildedSquids;
	}
	/**
	 * 翻译
	 */
	public TSquid doTranslateExceptionSquid(ExceptionSquid dc){
		TExceptionSquid squid = new TExceptionSquid();
		TSquid prevTSquid=this.getCurLastTSqud();
		if (prevTSquid == null) {
			Squid prevSquid = this.ctx.getPrevSquids(dc).get(0);
			prevTSquid = this.ctx.getSquidOut(prevSquid,TTransformationSquid.class);
            ((TExceptionDataSquid)prevTSquid).setExceptionHandle(true);
		}
		squid.setSquidId(dc.getId());
		squid.setName(dc.getName());
		squid.setPreviousSquid(prevTSquid);
		return squid;
	}
	
	/**
	 * 
	 * @param dc
	 * @return
	 */
	private TSquid doTranslateTransformation(ExceptionSquid dc) {
		TTransformationSquid ts = new TTransformationSquid();
		List<TTransformationAction> actions = new ArrayList<TTransformationAction>();
		ts.settTransformationActions(actions);
		for (TransformationLink link : dc.getTransformationLinks()) {
			Transformation from_trs = this.getTrsById(link.getFrom_transformation_id());
			Transformation to_trs = this.getTrsById(link.getTo_transformation_id());
			TTransformationAction action = new TTransformationAction();
			TTransformation tTransformation = new TTransformation();
			tTransformation.setType(TransformationTypeEnum.VIRTUAL);
			tTransformation.setSequenceIndex(1);
			tTransformation.setInKeyList(new JList<>(-from_trs.getColumn_id()));
			tTransformation.setOutKeyList(new JList<>(to_trs.getColumn_id()));
	
			Column col = this.getColumnById(to_trs.getColumn_id());
			JMap infoMap = new JMap<>();
			infoMap.put(TTransformationInfoType.STRING_MAX_LENGTH.dbValue, col.getLength());
			infoMap.put(TTransformationInfoType.NUMERIC_PRECISION.dbValue, col.getPrecision());
			infoMap.put(TTransformationInfoType.NUMERIC_SCALE.dbValue, col.getScale());
			tTransformation.setInfoMap(infoMap);
			
			HashSet<Integer> dtk = getDropedTrsKeys(to_trs);
			dtk.add(-from_trs.getColumn_id());
			action.setRmKeys(dtk);

			action.settTransformation(tTransformation);
			actions.add(action);
		}
		
		Column errorCol = getErrorCol();
		if (errorCol != null) {
			TTransformationAction action = new TTransformationAction();
			TTransformation tTransformation = new TTransformation();
			tTransformation.setType(TransformationTypeEnum.VIRTUAL);
			tTransformation.setSequenceIndex(1);
			tTransformation.setInKeyList(new JList<Integer>(TTransformationSquid.ERROR_KEY));
			tTransformation.setOutKeyList(new JList<Integer>(errorCol.getId()));
			action.settTransformation(tTransformation);
			HashSet<Integer> rmKeys = new HashSet<>();
			rmKeys.add(TTransformationSquid.ERROR_KEY);
			action.setRmKeys(rmKeys);
			
			JMap infoMap = new JMap<>();
			infoMap.put(TTransformationInfoType.STRING_MAX_LENGTH.dbValue, errorCol.getLength());
			infoMap.put(TTransformationInfoType.NUMERIC_PRECISION.dbValue, errorCol.getPrecision());
			infoMap.put(TTransformationInfoType.NUMERIC_SCALE.dbValue, errorCol.getScale());
			tTransformation.setInfoMap(infoMap);
			actions.add(action);
		}
		ts.setSquidId(dc.getId());
		ts.setName(dc.getName());
		
		ts.setPreviousSquid(this.getCurLastTSqud());
		return ts;
	}

	private Column getErrorCol() {
		ExceptionSquid de = (ExceptionSquid) this.currentSquid;
		for (Column col : de.getColumns()) {
			if (col.getName().toLowerCase().equals("error")) {
				return col;
			}
		}
		return null;
	}

	/**
	 * 抛弃transformation.并且返回可用的抛弃值。
	 * 
	 * @param tf
	 * @return
	 */
	protected HashSet<Integer> getDropedTrsKeys(Transformation tf) {
		HashSet<Integer> ret = new HashSet<>();
		if (!this.removeNoUseKeys) {
			ExceptionSquid ss = (ExceptionSquid) this.currentSquid;
			for (ReferenceColumn col : ss.getSourceColumns()) {
				boolean contains = false;
				for (Transformation t : this.getBeginTrs()) {
					if (t.getColumn_id() == col.getColumn_id()) {
						contains = true;
					}
				}
				if (!contains) {
					ret.add(col.getColumn_id());
				}
			}
			this.removeNoUseKeys = true;

		}
		return ret;
	}
}

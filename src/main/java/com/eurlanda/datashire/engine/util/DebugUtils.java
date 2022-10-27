package com.eurlanda.datashire.engine.util;

import cn.com.jsoft.jframe.utils.ClassUtils;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDebugSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.entity.TTransformationAction;
import com.eurlanda.datashire.engine.entity.TTransformationSquid;

import java.util.List;
import java.util.Set;

public class DebugUtils {
	public static void printSquidFlow(TSquidFlow tf){
		System.out.println(String.format("==================== SquidFlowID[%s] debugMode[%s] ===============",tf.getId(), tf.isDebugModel()));
		for(TSquid squid : tf.getSquidList()){
			String prevSquid = "";
			TSquid pre = null;
			try {
				pre= (TSquid) ClassUtils.getFieldValue(squid, "previousSquid");
				prevSquid = pre.getType()+"";
			} catch (Exception e) {
				try {
					pre = (TSquid) ClassUtils.getFieldValue(squid, "preSquid");
					prevSquid = pre.getType()+"";
				} catch (Exception e1) {
					try {
						prevSquid =ClassUtils.getFieldValue(squid, "preSquidList").toString();
					} catch (Exception e2) {
						
					}
					
				}
				
			}
			System.out.println(String.format("[%s] \t FromSquid:[name:%s,id:%s] \t PrevTSquid:[%s]", 
					squid.getType()+"",
					squid.getName(),
					squid.getSquidId(),
					prevSquid
			));
			if(squid instanceof TDebugSquid){
				TDebugSquid tds =((TDebugSquid) squid); 
				List<TSquid> dep =tds.getDependenceSquids();
				System.out.println(String.format("\t|----- break[%s] dataViewer[%s] depends[%s]",
						tds.isBreakPoint(),
						tds.isDataView(),
						dep==null?"":dep)
					);
			}else if(squid instanceof TTransformationSquid){
				List<TTransformationAction> actions = ((TTransformationSquid) squid).gettTransformationActions();
				for(TTransformationAction action: actions){
					System.out.println(String.format("\t|----- action[%s:%s] inkey[%s] outkey[%s] removedKeys[%s]", 
							action.gettTransformation().getName(),
							action.gettTransformation().getType(),
							action.gettTransformation().getInKeyList(),
							action.gettTransformation().getOutKeyList(),
							action.getRmKeys()
					));
				}
			}else if(squid instanceof TDataFallSquid){
				System.out.println("\tDataSource:"+((TDataFallSquid) squid).getDataSource());
				Set<TColumn> cols = ((TDataFallSquid) squid).getColumnSet();
				System.out.println("\tColumns");
				for(TColumn col: cols){
					System.out.println("\t|----- "+col);
				}
			}
			
			System.out.println();
		}
	}
}

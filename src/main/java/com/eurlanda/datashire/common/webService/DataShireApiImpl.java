package com.eurlanda.datashire.common.webService;

import com.eurlanda.datashire.engine.service.EngineService;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;

import javax.jws.WebService;
import java.util.ArrayList;
import java.util.HashMap;

@WebService
public class DataShireApiImpl implements DataShireApi
{
	@Override
	public ReturnMessage startSquidFlow(String token, Integer squid_flow_id,
										String call_bark_url, ArrayList<SquidEdit> squidEditList,
										HashMap<String, String> params) {
//		if(SquidFlowLauncher.runningSquidFlows!=null) {
//			for (TSquidFlow tsf : SquidFlowLauncher.runningSquidFlows) {
//				if (tsf.getId() == squid_flow_id) {
//					ReturnMessage rm = new ReturnMessage();
//					rm.setCode(104);
//					rm.setContent("业务流程正在执行中");
//					return rm;
//				}
//			}
//		}
		//WEB_SERVICE.TOKEN
		ReturnMessage rm = new ReturnMessage();

		if(token==null||!ConfigurationUtil.getWebServiceToken().equals(token)){
			rm.setCode(102);
			rm.setContent("token不匹配");
		}
		try {
			String taskId=null;
//			try {
//				updateProperty(squid_flow_id,SquidEditList);
//			}catch (Exception e){
//				rm.setCode(103);
//				rm.setContent("改变属性异常");
//				throw e;
//			}
//			try {
//				updateParam(squid_flow_id, params);
//			}catch (Exception e){
//				rm.setCode(102);
//				rm.setContent("改变变量异常");
//				throw e;
//			}
			taskId = EngineService.launchJob(squid_flow_id,call_bark_url,squidEditList,params);
			rm.setCode(0);
			rm.setTaskId(taskId);
		}catch(Exception e){
			e.printStackTrace();
			rm.setCode(101);
			rm.setContent("未知异常");
		}
		return rm;
	}

//	private int updateProperty(Integer squid_flow_id,ArrayList<SquidEdit> SquidEditList){
//		for(int i =0;i<SquidEditList.size();i++){
//			SquidEdit se1 = SquidEditList.get(i);
//			List<UpdateProperty> listUpdateProperty = new ArrayList<UpdateProperty>();
//			UpdateProperty up = new UpdateProperty();
//			up.setProperty(se1.getPropertyName());
//			up.setValue(se1.getValue().toString());
//			listUpdateProperty.add(up);
//			for(int j =i+1;j<SquidEditList.size();j++){
//				SquidEdit se2 = SquidEditList.get(j);
//				if(se2.getSuiqidName()!=null&&se2.getSuiqidName()
    // .equals(se1.getSuiqidName())&&(se2.getTransformationId()==null||se2.getTransformationId()==se1.getTransformationId().intValue())){
//					up = new UpdateProperty();
//					up.setProperty(se2.getPropertyName());
//					up.setValue(se2.getValue().toString());
//					listUpdateProperty.add(up);
//					j--;
//					i--;
//					SquidEditList.remove(j);
//				}
//			}
//			squidFlowDao.updateProperty(squid_flow_id, se1.getSuiqidName(), "DS_SQUID", listUpdateProperty);
//		}
//		return 0;
//	}
//	private int updateParam(Integer squid_flow_id,HashMap<String, String> params){
//		List<DSVariable> listDSVariable = squidFlowDao.getVariableList(squid_flow_id);
//		if(params!=null) {//存在则开始执行
//			for (DSVariable dSVariable : listDSVariable) {//循环结果值
//				Object value = params.get(dSVariable.getVariable_name());
//				if (value != null) {//需要被改变
//					SystemDatatype ds = SystemDatatype.parse(dSVariable.getVariable_type());//获得类型
//					TDataType tdt = TDataType.sysType2TDataType(dSVariable.getVariable_type());//获得类型
//					if(ds.value()!=SystemDatatype.DATETIME.value()) {
//						Object o = ImplicitInstalledUtil.implicitInstalled(tdt, ds, value, dSVariable.getVariable_precision(), dSVariable.getVariable_scale());
//						dSVariable.setVariable_value(o.toString());
//					}else {
//						dSVariable.setVariable_value(value.toString());
//					}
//					squidFlowDao.updateVariableValue(dSVariable);
//				}
//			}
//		}
//		return 0;
//	}
}

package com.eurlanda.datashire.engine;

import com.eurlanda.datashire.common.rpc.impl.EngineServiceImpl;
import org.apache.avro.AvroRemoteException;
/**
 * 引擎测试。
 * @author Gene
 *
 */
public class EngineTest {
	public static void main(String[] args) {
		EngineServiceImpl esi = new EngineServiceImpl();
		try {
			esi.launchEngineJob(111, 3, "", "");
		} catch (AvroRemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		org.apache.hadoop.util.PlatformName pn;
	}

	@org.junit.Test
    public void test1() {


		System.out.println(Math.ceil(0.1));

    }
}

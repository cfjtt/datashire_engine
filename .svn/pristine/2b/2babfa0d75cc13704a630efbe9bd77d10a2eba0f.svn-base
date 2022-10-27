//package com.eurlanda.datashire.engine.translation.sqlparser;
//
//import javassist.ClassPool;
//import javassist.CtClass;
//import javassist.CtMethod;
//
//public class ClassT {
//	public static void main(String[] args) throws Exception {
//		CtClass cls = ClassPool.getDefault().makeClass("Testhaha");
//		CtMethod method = CtMethod.make("public boolean execute(){return 5>1 && 4<8 &&( 2>5 ||9>7);}", cls);
//		cls.addMethod(method);
//		String $sdf="sdf";
//
//		CtClass[] cls2 = new CtClass[1];
//		cls2[0]=ClassPool.getDefault().get("com.eurlanda.datashire.engine.translation.sqlparser.IExpressionExecutor");
//		cls.setInterfaces(cls2);
//		cls.writeFile();
//		IExpressionExecutor o = (IExpressionExecutor) cls.toClass().newInstance();
//
//		long begin = System.currentTimeMillis();
//		for (int i = 0; i < 100000; i++) {
//			o.execute();
//		}
//		System.out.println(" time used (ms):" + (System.currentTimeMillis() - begin));
//
//		//System.out.println(o);
//	}
//}

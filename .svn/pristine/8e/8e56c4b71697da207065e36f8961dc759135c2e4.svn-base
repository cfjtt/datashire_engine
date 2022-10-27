package com.eurlanda.datashire.engine.translation.expression;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

public class ValidatorBuilder {
	public static void main(String[] args) throws Exception {
		ClassPool cp = ClassPool.getDefault();
		CtClass cls = cp.makeClass("DefaultValidator1");
		cp.importPackage("java.util.Map");
		CtMethod method = CtMethod.make("public boolean validate(Map data){return 5>1 && 4<8 &&( 2>5 ||9>7);}", cls);
		cls.addMethod(method);
		CtClass[] cls2 = new CtClass[1];
		cls2[0]=ClassPool.getDefault().get("com.eurlanda.datashire.engine.translation.expression.IExpressionValidator");
		cls.setInterfaces(cls2);
		cls.writeFile();
		IExpressionValidator o = (IExpressionValidator) cls.toClass().newInstance();
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 100000000; i++) {
			o.validate(null);
		}
		System.out.println(" time used (ms):" + (System.currentTimeMillis() - begin));
		
	}
}

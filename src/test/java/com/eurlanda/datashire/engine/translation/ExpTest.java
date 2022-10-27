package com.eurlanda.datashire.engine.translation;

import org.junit.Test;
import org.nfunk.jep.EvaluatorVisitor;
import org.nfunk.jep.type.NULL;


public class ExpTest {
	public static void main(String[] args) throws Exception {
			
		org.nfunk.jep.JEP myParser = new org.nfunk.jep.JEP();
		int i;
		String exp = "#a>1 and (#c==5 or b2.dd==0.3)";
		
		exp = exp.replaceAll("(?i)and", " && ").replaceAll("(?i)or", " || ").replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$");
		System.out.println(exp);
		
		myParser.addVariable("null", new Object());
		myParser.addVariable("$a", null);
		myParser.addVariable("$c", null);
		myParser.addVariable("b2$dd", null);
		myParser.parseExpression(exp);

		EvaluatorVisitor ev = myParser.getEvaluatorVisitor();
		ev.setTrapNullValues(false);

		long start=System.currentTimeMillis();
//		for(i = 0; i< 2; i++){
			myParser.addVariable("$a", 1);
//			myParser.addVariable("$c", 5);
			myParser.addVariable("b2$dd", 0.3);
			System.out.println( myParser.getValue());
		myParser.getOperatorSet();
		System.out.println(myParser.getErrorInfo());
//		}
		System.out.println("time used:"+(System.currentTimeMillis()-start));
		System.out.println("done!");
			
			
	}

	@Test
	public void test2() {
		org.nfunk.jep.JEP myParser = new org.nfunk.jep.JEP();
		int i;
		String exp = "$a>null  &&  ($c==5  ||  b2$dd==0.3)";

//        exp = exp.replaceAll("(?i)and", " && ").replaceAll("(?i)or", " || ").replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$");
		System.out.println(exp);

		myParser.addVariable("null", NULL.build());
		myParser.addVariable("$a", 2);
		myParser.addVariable("$c", NULL.build());
		myParser.addVariable("b2$dd", 0.2);
		myParser.parseExpression(exp);

		EvaluatorVisitor ev = myParser.getEvaluatorVisitor();
		ev.setTrapNullValues(false);

		long start=System.currentTimeMillis();
//		for(i = 0; i< 2; i++){
		myParser.addVariable("$a", 2);
		myParser.addVariable("$c", NULL.build());
		myParser.addVariable("b2$dd", 0.3);
		System.out.println( myParser.getValue());
//		}
		System.out.println("time used:"+(System.currentTimeMillis()-start));
		System.out.println("done!");
	}
}
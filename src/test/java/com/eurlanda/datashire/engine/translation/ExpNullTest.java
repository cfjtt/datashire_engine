package com.eurlanda.datashire.engine.translation;

public class ExpNullTest {
	static class NullObject{
		private boolean NULL = true;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (NULL ? 1231 : 1237);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			NullObject other = (NullObject) obj;
			if (NULL != other.NULL)
				return false;
			return true;
		}
		
	}
	public static void main(String[] args) throws Exception {
			
		org.nfunk.jep.JEP jep = new org.nfunk.jep.JEP();
		
		  jep.addConstant("null",new NullObject()); // define a constant in the parser
		  jep.addVariable("x",new NullObject()); // add a variable with the NULL value
		  jep.parseExpression("x==null"); // Compare a variable to the NULL value
		  
		System.out.println(jep.getValue());
			
	}
}
package com.eurlanda.common;

import org.junit.Test;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by zhudebin on 15-4-2.
 */
public class JavaScriptTest {

    public void test1() {
        org.codehaus.jackson.JsonNode jn;
        org.apache.hadoop.hbase.ipc.RpcControllerFactory rd;
        org.apache.commons.configuration.Configuration c;
        com.google.protobuf.ServiceException se;
        org.antlr.runtime.RecognitionException re;
        org.apache.commons.io.output.DeferredFileOutputStream dfo;
    }

    @Test
    public void testScript1() {

        ScriptEngineManager sem = new ScriptEngineManager();    /*script引擎管理*/
        ScriptEngine se = sem.getEngineByName("javascript");           /*script引擎*/
        try {
            se.eval(" var strname = 'Key' ");                     /* 执行一段script */
            se.eval("function sayHello(   ) { "
                    + " print('Hello '+strname+'!');return 'my name is '+strname;"
                    + "}");   /* 添加一个方法*/
            Invocable invocableEngine = (Invocable) se;
            String callbackvalue = (String) invocableEngine.invokeFunction("sayHello");   /*调用方法中的函数*/
            Object obj = invocableEngine.invokeFunction("sayHello");   /*调用方法中的函数*/
            System.out.println(callbackvalue);              /** 打印返回值*/
            System.out.println(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScript2() {
        ScriptEngineManager sem = new ScriptEngineManager();    /*script引擎管理*/
        ScriptEngine se = sem.getEngineByName("javascript");           /*script引擎*/
        try {
            se.eval("function add(aa,bb) { "
                    + " println(typeof aa);"
                    + " println(typeof bb);"
                    + " println(bb.getHours()) ;"
                    + " return aa + bb;"
                    + "}");   /* 添加一个方法*/
            Invocable invocableEngine = (Invocable) se;
//            Object obj = invocableEngine.invokeFunction("add", 1, "111");   /*调用方法中的函数*/
//            Object obj = invocableEngine.invokeFunction("add", 1, 11);   /*调用方法中的函数*/
//            Object obj = invocableEngine.invokeFunction("add", 1, new Date(new java.util.Date().getTime()));   /*调用方法中的函数*/
//            Object obj = invocableEngine.invokeFunction("add", 1, new java.util.Date());   /*调用方法中的函数*/
            Object obj = invocableEngine.invokeFunction("add", true, new java.util.Date());   /*调用方法中的函数*/
            System.out.println(obj);
            obj = invocableEngine.invokeFunction("add", true, new Timestamp(new java.util.Date().getTime()));   /*调用方法中的函数*/
            System.out.println(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScriptNumber() {
        int i = 1;
        double d = i;
//        i = d;
        ScriptEngineManager sem = new ScriptEngineManager();    /*script引擎管理*/
        ScriptEngine se = sem.getEngineByName("javascript");           /*script引擎*/
        try {
            se.eval("function add(aa,bb) { "
                    + " println(typeof aa);"
                    + " println(typeof bb);"
                    + " return aa + bb;"
                    + "}");   /* 添加一个方法*/
            Invocable invocableEngine = (Invocable) se;
//            Object obj = invocableEngine.invokeFunction("add", 1, 22);   /*调用方法中的函数*/
//            Object obj = invocableEngine.invokeFunction("add", 1L, 22.0);   /*调用方法中的函数*/
            Object obj = invocableEngine.invokeFunction("add", new BigDecimal("1.222333343434"), 22.0);   /*调用方法中的函数*/
            System.out.println(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScriptBoolean() {
        ScriptEngineManager sem = new ScriptEngineManager();    /*script引擎管理*/
        ScriptEngine se = sem.getEngineByName("javascript");           /*script引擎*/
        try {
            se.eval("function add(aa) { "
                    + " if(aa){println('---true');} else {println('----false')};"
                    + " println(typeof aa);"
                    + " return aa;"
                    + "}");   /* 添加一个方法*/
            Invocable invocableEngine = (Invocable) se;
//            Object obj = invocableEngine.invokeFunction("add", true);   /*调用方法中的函数*/
            Object obj = invocableEngine.invokeFunction("add", false);   /*调用方法中的函数*/
            System.out.println(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScriptNull() {
        ScriptEngineManager sem = new ScriptEngineManager();    /*script引擎管理*/
        ScriptEngine se = sem.getEngineByName("javascript");           /*script引擎*/
        try {
            se.eval("function add(aa) { "
                    + " println(typeof aa);"
                    + " return aa;"
                    + "}");   /* 添加一个方法*/
            Invocable invocableEngine = (Invocable) se;
//            Object obj = invocableEngine.invokeFunction("add", true);   /*调用方法中的函数*/
            Object obj = invocableEngine.invokeFunction("add", null);   /*调用方法中的函数*/
            System.out.println(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScriptArray() {
        ScriptEngineManager sem = new ScriptEngineManager();    /*script引擎管理*/
        ScriptEngine se = sem.getEngineByName("javascript");           /*script引擎*/
        try {
            se.eval("function add(aa) { "
                    + " println(aa);"
                    + " println(typeof aa);"
                    + " for (var p in aa) {" +
                    "println(p);" +
                    "} "
                    + " return aa[0];"
                    + "}");   /* 添加一个方法*/
            Invocable invocableEngine = (Invocable) se;
//            Object obj = invocableEngine.invokeFunction("add", new String[]{"1","2"});   /*调用方法中的函数*/
            Object obj = invocableEngine.invokeFunction("add", new int[]{1,2,3});   /*调用方法中的函数*/
            System.out.println(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScriptDate() {
        ScriptEngineManager sem = new ScriptEngineManager();    /*script引擎管理*/
        ScriptEngine se = sem.getEngineByName("javascript");           /*script引擎*/
        try {
            se.eval("function add(aa) { "
                    + " println(typeof aa);"
//                    + " println(aa.getYear());"
//                    + " println(aa.getMonth());"
//                    + " println(aa.getDate());"
                    + " println(aa.getHours());"
                    + " println(aa.getMinutes());"
                    + " println(aa.getSeconds());"
//                    + " println(aa.getMilliseconds());"
                    + " println(aa.getTime());"
                    + " println(typeof aa);"
                    + " return aa;"
                    + "}");   /* 添加一个方法*/
            Invocable invocableEngine = (Invocable) se;
            // java.sql.Timestamp
//            Object obj = invocableEngine.invokeFunction("add", new Timestamp(new java.util.Date().getTime()));   /*调用方法中的函数*/
            // java.sql.Date
//            Object obj = invocableEngine.invokeFunction("add", new Date(new java.util.Date().getTime()));   /*调用方法中的函数*/
            // java.sql.Time
            Object obj = invocableEngine.invokeFunction("add", new Time(new java.util.Date().getTime()));   /*调用方法中的函数*/

            System.out.println(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印Script引擎
     */
    public static void printAllEngine() {
        ScriptEngineManager manager = new ScriptEngineManager();
        List<ScriptEngineFactory> factoryList = manager.getEngineFactories();
        for (ScriptEngineFactory factory : factoryList) {
            System.out.println(factory.getEngineName());
            System.out.println(factory.getEngineVersion());
            System.out.println(factory.getLanguageName());
            System.out.println(factory.getLanguageVersion());
            System.out.println(factory.getExtensions());
            System.out.println(factory.getMimeTypes());
            System.out.println(factory.getNames());
        }
    }

    /**
     * 执行JavaScript代码
     */
    public static void exeJSForCode() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");
        String script = "print ('http://blog.163.com/nice_2012/')";
        try {
            engine.eval(script);
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行JavaScript文件代码
     */
    public static void exeJSForFile() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");
        try {
            FileReader reader = new FileReader("file.js");
            engine.eval(reader);
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Binding AND Exception
     */
    public static void exeJSForBinding() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");
        engine.put("a", 1);
        engine.put("b", 5);
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        Object a = bindings.get("a");
        Object b = bindings.get("b");
        System.out.println("a = " + a);
        System.out.println("b = " + b);
        try {
            Object result = engine.eval("c = a + b;");
            System.out.println("a + b = " + result);
        } catch (ScriptException e) {
            e.printStackTrace();
        }

    }

    /**
     * Function
     */
    public static void exeJSForFunction() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");
        try {
            engine.eval("function add (a, b) {c = a + b; return c; }");
            Invocable jsInvoke = (Invocable) engine;
            Object result1 = jsInvoke.invokeFunction("add", new Object[] { 10,
                    5 });
            System.out.println(result1);
            Adder adder = jsInvoke.getInterface(Adder.class);
            int result2 = adder.add(10, 5);
            System.out.println(result2);
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

    }

    /**
     * Compilable
     */
    public static void exeJSForCompilable() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");
        Compilable jsCompile = (Compilable) engine;
        try {
            CompiledScript script = jsCompile
                    .compile("function hi () {print ('hello" + "'); }; hi ();");
            for (int i = 0; i < 5; i++) {
                script.eval();
            }
        } catch (ScriptException e) {
            e.printStackTrace();
        }

    }

    /**
     * 执行 JavaScript
     */
    public static void exeJavaScript() {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("javascript");
        try {
            Double hour = (Double) engine.eval("var date = new Date();"
                    + "date.getHours();");
            String msg;
            if (hour < 10) {
                msg = "Good morning";
            } else if (hour < 16) {
                msg = "Good afternoon";
            } else if (hour < 20) {
                msg = "Good evening";
            } else {
                msg = "Good night";
            }
            System.out.println(hour);
            System.out.println(msg);
        } catch (ScriptException e) {
            System.err.println(e);
        }
    }

    interface Adder {

        int add(int a, int b);

    }
}

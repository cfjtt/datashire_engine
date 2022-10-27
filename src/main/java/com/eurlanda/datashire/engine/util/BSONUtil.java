package com.eurlanda.datashire.engine.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhudebin on 15-5-20.
 */
public class BSONUtil {

    public static Object getByKey(BasicDBObject bsonObject, String[] keys, int idx) {

        if(bsonObject.containsField(keys[idx])) {
            Object obj = bsonObject.get(keys[idx]);
            if(idx == keys.length-1) {  // 最后一层，返回
                return obj;
            } else {
                if(obj instanceof BasicDBObject) {
                    return getByKey((BasicDBObject)obj, keys, idx+1);
                } else if(obj instanceof BasicDBList){
                    return getByKey((BasicDBList)obj, keys, idx+1);
                } else {
                    // 既不是对象，也不是数组，还需要选择下一级，因此，数据异常
                    return null;
                }
            }
        } else {
            return null;
        }

    }

    private static Object getByKey(BasicDBList list, String[] keys, int idx) {
        String key = keys[idx];
        try {
            int i = Integer.parseInt(key);
            if(list.size()>i) {
                Object obj = list.get(i);
                if(idx == keys.length-1) {
                    return obj;
                } else {
                    if(obj instanceof BasicDBObject) {
                        return getByKey((BasicDBObject)obj, keys, idx+1);
                    } else if(obj instanceof BasicDBList){
                        return getByKey((BasicDBList)obj, keys, idx+1);
                    } else {
                        // 既不是对象，也不是数组，还需要选择下一级，因此，数据异常
                        return null;
                    }
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public static void main(String[] args) {

//        insertData();

        /**
        Object obj = getByKey(queryData(), "k2.2.age".split("\\."), 0);
        System.out.println(obj);
         */
        BasicDBObject obj = (BasicDBObject)queryData();
        System.out.println(obj);
        System.out.println("----------------------------------");
        System.out.println(obj.get("k2.2.addr"));
        System.out.println("----------------------------------");
        Object o = getByKey(obj, "k2.2.addr".split("\\."), 0);
        System.out.println(o);
    }

    private static DBObject queryData() {
        DB db = null;
        MongoClient mongo;
        DBCollection tcollection;
        MongoClientURI connectionString = new MongoClientURI("mongodb://192.168.137.16:27017");
        mongo = new MongoClient(connectionString);
        db = mongo.getDB("test");
        tcollection = db.getCollection("test2");
        DBObject dbObject = tcollection.find().next();
        return dbObject;
    }

    private static void insertData() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("k1", "k1");
        Map<String, Object> m2 = new HashMap<String, Object>();
        m2.put("k1","k1");
        m2.put("k2", new Person("n1", 10, new String[]{"shanghai", "changsha"}).toMap());
        map.put("k2", Arrays.asList(new Object[]{"a",100, new Person("n2", 20, new String[]{"shanghai2", "changsha2"}).toMap()}));
        map.put("k3", m2);
        map.put("k4", Arrays.asList(new String[]{"a","b","c","d"}));

        MongoDatabase db = null;
        MongoClient mongo;
        MongoCollection<Document> tcollection;
        MongoClientURI connectionString = new MongoClientURI("mongodb://192.168.137.16:27017");
        mongo = new MongoClient(connectionString);
        db = mongo.getDatabase("test");
        tcollection = db.getCollection("test2");
        tcollection.insertOne(new Document(map));
    }


    static class Person {
        String name;
        int age;
        String[] addr;

        public Person(String name, int age, String[] addr) {
            this.name = name;
            this.age = age;
            this.addr = addr;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("name", name);
            map.put("age", age);
            map.put("addr", Arrays.asList(addr));
            return map;
        }
    }
}

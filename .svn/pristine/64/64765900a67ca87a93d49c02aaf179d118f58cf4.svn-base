package com.eurlanda.datashire.engine.mongodb;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.Test;

/**
 * Created by zhudebin on 2016/12/7.
 */
public class MongodbTest {

    @Test
    public void testQueryString() {
//        String queryStr = "{name:/张三/}";
        String queryStr = "{name:{$regex:\"yiibai.com\"}}";

        DBObject obj = (DBObject) JSON.parse(queryStr);

        System.out.println(obj);

    }

}

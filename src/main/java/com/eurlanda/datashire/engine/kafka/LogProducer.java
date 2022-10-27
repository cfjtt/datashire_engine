package com.eurlanda.datashire.engine.kafka;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.engine.entity.SFLog;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.Json;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.getKafkaBrokerList;
import static com.eurlanda.datashire.engine.util.ConfigurationUtil.getKafkaLogTopic;

/**
 * Created by zhudebin on 14-5-30.
 */
public class LogProducer {

    private static Log log = LogFactory.getLog(LogProducer.class);
    private static Producer<String, String> producer;
    // 是否开启kafka Log,true:开启；false:关闭
    private static boolean isLog = true;

    private static Producer<String, String> getProducer() {
        if(producer == null) {
            synchronized (LogProducer.class) {
                if(producer == null) {
                    Properties props = new Properties();
                    props.setProperty("metadata.broker.list",getKafkaBrokerList());
                    props.setProperty("serializer.class","kafka.serializer.StringEncoder");
                    props.put("request.required.acks","1");
                    ProducerConfig config = new ProducerConfig(props);
                    producer = new Producer<>(config);
                }
            }
        }
        return producer;
    }

    public static void sendLog(SFLog sfLog) {
        log.debug(JSON.toJSONString(sfLog));
        if(isLog) {
            try {
                Producer<String, String> producer = getProducer();
                KeyedMessage<String, String> data = new KeyedMessage<>(getKafkaLogTopic(), JSON.toJSONString(sfLog));
                producer.send(data);
            }catch (Exception e) {
                log.error("kafka日志发送异常", e);
            }
        }
    }

    public static void sendLog(List<SFLog> sfLogs) {
        try {
            Producer<String, String> producer = getProducer();
            List<KeyedMessage<String, String>> datas = new ArrayList<>();
            for (SFLog sfLog : sfLogs) {
                datas.add(new KeyedMessage<String, String>(getKafkaLogTopic(), Json.encode(sfLog)));
            }
            producer.send(datas);
        }catch (Exception e) {
            log.error("kafka日志发送异常", e);
        }
    }

}

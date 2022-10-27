package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.util.ESSquidUtil;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

/**
 * Created by zhudebin on 15-10-12.
 */
public class TDestESSquid extends TSquid {

    private TSquid previousSquid;
    private String path;
    private String es_nodes;
    private String es_port;
    private Map<Integer, String> id2name;
    private String is_mapping_id;

    public TDestESSquid() {
        this.setType(TSquidType.DEST_ES_SQUID);
    }

    @Override protected Object run(JavaSparkContext jsc) throws EngineException {
        ESSquidUtil.saveToES(jsc.sc(), path, es_nodes, es_port, previousSquid.outRDD, id2name, is_mapping_id);
        return null;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getEs_nodes() {
        return es_nodes;
    }

    public void setEs_nodes(String es_nodes) {
        this.es_nodes = es_nodes;
    }

    public String getEs_port() {
        return es_port;
    }

    public void setEs_port(String es_port) {
        this.es_port = es_port;
    }

    public Map<Integer, String> getId2name() {
        return id2name;
    }

    public void setId2name(Map<Integer, String> id2name) {
        this.id2name = id2name;
    }

    public String getIs_mapping_id() {
        return is_mapping_id;
    }

    public void setIs_mapping_id(String is_mapping_id) {
        this.is_mapping_id = is_mapping_id;
    }
}

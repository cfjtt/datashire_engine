package com.eurlanda.datashire.engine.translation;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhudebin on 16/3/17.
 */
public class IdKeyGenerator {

    private Set<Integer> excludeKeys = new HashSet<>();
    private AtomicInteger currentId = new AtomicInteger(1);

    public synchronized int genKey() {
        int id = currentId.incrementAndGet();
        while(excludeKeys.contains(id)) {
            id = currentId.incrementAndGet();
        }
        return id;
    }

    public void addExcludeKey(int key) {
        excludeKeys.add(key);
    }

}

package com.bawi.spark.common;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;

public class CustomMapAccumulator extends AccumulatorV2<String, HashMap<String, Long>> {
    private HashMap<String, Long> map;

    public CustomMapAccumulator() {
        this(new HashMap<>());
    }

    public CustomMapAccumulator(HashMap<String, Long> map) {
        this.map = map;
    }

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<String, HashMap<String, Long>> copy() {
        return new CustomMapAccumulator(map);
    }

    @Override
    public void reset() {
        map = new HashMap<>();
    }

    @Override
    public void add(String key) {
        Long count = map.getOrDefault(key, 0L);
        map.put(key, count + 1L);
    }

    @Override
    public void merge(AccumulatorV2<String, HashMap<String, Long>> other) {
        other.value().forEach((k, v) -> {
            Long count = map.getOrDefault(k, 0L);
            map.put(k, count + v);
        });
    }

    @Override
    public HashMap<String, Long> value() {
        return map;
    }
}

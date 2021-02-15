package com.bawi.spark.common;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class SparkBase implements SparkContextListenerRegistrar, SparkMetricsRegistrar {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBase.class);
    public static final String CUSTOM_MAP_ACCUMULATOR_NAME = "customMapAccumulator";
    protected Map<String, CustomMapAccumulator> customMapAccumulatorMap = new HashMap<>();

    private SparkSession sparkSession;

    public SparkBase() {
        this(SparkSession.builder().appName(SparkBase.class.getSimpleName()).getOrCreate());
    }

    public SparkBase(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    protected abstract void doInRun(SparkSession sparkSession);

    public void run() {
        long startTimeMillis = System.currentTimeMillis();
        CustomMapAccumulator customMapAccumulator = getCustomMapAccumulator();
        sparkSession.sparkContext().register(customMapAccumulator, CUSTOM_MAP_ACCUMULATOR_NAME);
        try {
            setupMetrics(sparkSession, customMapAccumulatorMap);

            getOnStartListeners().forEach(l -> l.accept(sparkSession));

            doInRun(sparkSession);

            getOnSuccessListeners().forEach(l -> l.accept(sparkSession));

            customMapAccumulator.add("ApplicationStatus.SUCCESS");

            LOGGER.info("Spark processing succeeded after {} ms", System.currentTimeMillis() - startTimeMillis);
        } catch (Exception e) {
            getOnErrorListeners().forEach(l -> l.accept(sparkSession));
            customMapAccumulator.add("ApplicationStatus.FAILURE");
            LOGGER.error("Spark processing failed after " + (System.currentTimeMillis() - startTimeMillis) + " ms due to ", e);
            throw e;
        }
    }

    public CustomMapAccumulator getCustomMapAccumulator() {
        if (customMapAccumulatorMap.containsKey(CUSTOM_MAP_ACCUMULATOR_NAME)) {
            return customMapAccumulatorMap.get(CUSTOM_MAP_ACCUMULATOR_NAME);
        } else {
            CustomMapAccumulator customMapAccumulator = new CustomMapAccumulator();
            customMapAccumulatorMap.put(CUSTOM_MAP_ACCUMULATOR_NAME, customMapAccumulator);
            return customMapAccumulator;
        }
    }
}

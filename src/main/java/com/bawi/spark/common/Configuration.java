package com.bawi.spark.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    private final Properties properties = new Properties();

    public Configuration(String path) {
        try {
            InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(path);
            properties.load(resourceAsStream);
        } catch (IOException e) {
            LOGGER.error("Failed to load configuration from " + path, e);
        }
    }

    public String getString(String name) {
        return properties.getProperty(name);
    }

}

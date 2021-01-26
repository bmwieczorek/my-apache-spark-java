package com.bawi.spark.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    private Properties properties;

    public Configuration(Properties properties) {
        this.properties = properties;
    }

    public Configuration(String path) {
        try {
            InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(path);
            Properties properties = new Properties();
            properties.load(resourceAsStream);
            this.properties = properties;
        } catch (IOException e) {
            LOGGER.error("Failed to load configuration from " + path, e);
            throw new RuntimeException(e);
        }
    }

    public String getString(String name) {
        return properties.getProperty(name);
    }

}
